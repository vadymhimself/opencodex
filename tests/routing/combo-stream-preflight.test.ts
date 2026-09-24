import { describe, expect, spyOn, test } from "bun:test";
import {
  comboStreamPayloadCommitsOutput,
  deferProtocolSafeResetRecovery,
  preflightComboStreamResponse,
} from "../../src/server/responses/combo-stream-preflight";
import { stageCommitment, type RequestFailureStage } from "../../src/lib/request-failure-model";
import type { RequestLogContext } from "../../src/server/request-log";
import { MAX_CLIENT_SSE_FRAME_BYTES } from "../../src/server/sse-frame-buffer";

const sse = (...payloads: unknown[]): Response => new Response(
  payloads.map(payload => `data: ${JSON.stringify(payload)}\n\n`).join(""),
  { headers: { "content-type": "text/event-stream" } },
);

const preflightChunkLimit = Math.max(1, Math.ceil(MAX_CLIENT_SSE_FRAME_BYTES / 1024));

function prefixThenReadError(prefix: Uint8Array, error: Error): {
  response: Response;
  cancelSpy: () => ReturnType<typeof spyOn> | undefined;
} {
  let sentPrefix = false;
  let cancelSpy: ReturnType<typeof spyOn> | undefined;
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (!sentPrefix) {
        sentPrefix = true;
        controller.enqueue(prefix);
        return;
      }
      return Promise.reject(error);
    },
  });
  const originalGetReader = stream.getReader.bind(stream);
  stream.getReader = (() => {
    const reader = originalGetReader();
    cancelSpy = spyOn(reader, "cancel");
    return reader;
  }) as ReadableStream<Uint8Array>["getReader"];
  return {
    response: new Response(stream, { headers: { "content-type": "text/event-stream" } }),
    cancelSpy: () => cancelSpy,
  };
}

const createdPrefix = new TextEncoder().encode(`data: ${JSON.stringify({
  type: "response.created",
  response: { id: "r1", status: "in_progress" },
})}

`);

describe("combo stream preflight", () => {
  test("keeps only lifecycle preamble replayable and treats unknown output conservatively", () => {
    expect(comboStreamPayloadCommitsOutput({ type: "response.created" })).toBe(false);
    expect(comboStreamPayloadCommitsOutput({ type: "response.heartbeat" })).toBe(false);
    expect(comboStreamPayloadCommitsOutput({ type: "response.failed" })).toBe(false);
    expect(comboStreamPayloadCommitsOutput({ type: "response.incomplete" })).toBe(false);
    expect(comboStreamPayloadCommitsOutput({ type: "error" })).toBe(true);
    expect(comboStreamPayloadCommitsOutput({ type: "response.output_text.delta", delta: "x" })).toBe(true);
    expect(comboStreamPayloadCommitsOutput({ type: "response.output_item.added", item: { type: "function_call" } })).toBe(true);
    expect(comboStreamPayloadCommitsOutput({ type: "provider.future_event" })).toBe(true);
  });

  test("rejects a non-SSE success when caller requires a stream", async () => {
    let cancels = 0;
    const response = new Response(new ReadableStream<Uint8Array>({
      cancel() { cancels += 1; },
    }), { headers: { "content-type": "application/json" } });
    const result = await preflightComboStreamResponse(
      response,
      { model: "m1", provider: "a" },
      { expectedTransport: "sse" },
    );

    expect(result.kind).toBe("failed");
    expect(result.response.status).toBe(502);
    expect(cancels).toBe(1);
  });

  test("rejects malformed content blocks in canonical Anthropic JSON", async () => {
    const response = new Response(JSON.stringify({
      id: "msg_1",
      type: "message",
      role: "assistant",
      content: [null],
      model: "claude-opus-5",
      stop_reason: "end_turn",
      stop_sequence: null,
      usage: { input_tokens: 1, output_tokens: 1 },
    }), { headers: { "content-type": "application/json" } });
    const result = await preflightComboStreamResponse(
      response,
      { model: "m1", provider: "a" },
      { expectedTransport: "json" },
    );

    expect(result.kind).toBe("failed");
    expect(result.response.status).toBe(502);
  });

  test("converts a zero-output failed terminal into a retryable HTTP failure", async () => {
    const logCtx: RequestLogContext = { model: "m1", provider: "a" };
    const result = await preflightComboStreamResponse(sse(
      { type: "response.created", response: { id: "r1", status: "in_progress" } },
      {
        type: "response.failed",
        response: {
          id: "r1",
          status: "failed",
          error: { type: "server_error", code: "upstream_server_error", message: "busy" },
          usage: { input_tokens: 7, output_tokens: 0, total_tokens: 7 },
          provider_trace_id: "must-not-cross-the-combo-boundary",
        },
      },
    ), logCtx);

    expect(result.kind).toBe("failed");
    expect(result.response.status).toBe(502);
    const body = await result.response.json();
    expect(body).toMatchObject({
      error: { code: "upstream_server_error", message: "busy" },
      response: { usage: { input_tokens: 7, output_tokens: 0 } },
    });
    expect(JSON.stringify(body)).not.toContain("provider_trace_id");
  });

  test("converts zero-output transport incompletes into retryable HTTP failures", async () => {
    const cases = [
      ["adapter_eof", "Upstream stream ended unexpectedly without a terminal event", 502],
      ["missing_terminal_event", "Upstream incomplete", 502],
      ["upstream_stall_timeout", "Upstream stalled", 504],
    ] as const;
    for (const [reason, message, status] of cases) {
      const result = await preflightComboStreamResponse(sse(
        { type: "response.created", response: { id: "r1", status: "in_progress" } },
        {
          type: "response.incomplete",
          response: {
            id: "r1",
            status: "incomplete",
            incomplete_details: { reason },
            usage: { input_tokens: 11, output_tokens: 0, total_tokens: 11 },
          },
        },
      ), { model: "m1", provider: "a" });

      expect(result.kind).toBe("failed");
      expect(result.response.status).toBe(status);
      const body = await result.response.json();
      expect(body.error).toMatchObject({ type: "upstream_error", code: "upstream_server_error" });
      expect(body.error.message).toContain(message);
      expect(body.response.usage).toMatchObject({ input_tokens: 11, output_tokens: 0 });
    }
  });

  test("does not replay semantic incompletes that another provider cannot safely repair", async () => {
    const source = sse(
      { type: "response.created", response: { id: "r1", status: "in_progress" } },
      {
        type: "response.incomplete",
        response: {
          id: "r1",
          status: "incomplete",
          incomplete_details: { reason: "max_output_tokens" },
        },
      },
    );
    const expected = await source.clone().text();
    const result = await preflightComboStreamResponse(source, { model: "m1", provider: "a" });
    expect(result.kind).toBe("accepted");
    expect(await result.response.text()).toBe(expected);
  });

  test("does not replay transport incompletes after output commits the target", async () => {
    const source = sse(
      { type: "response.created", response: { id: "r1", status: "in_progress" } },
      { type: "response.output_text.delta", delta: "visible" },
      {
        type: "response.incomplete",
        response: {
          id: "r1",
          status: "incomplete",
          incomplete_details: { reason: "adapter_eof" },
        },
      },
    );
    const expected = await source.clone().text();
    const result = await preflightComboStreamResponse(source, { model: "m1", provider: "a" });
    expect(result.kind).toBe("accepted");
    expect(await result.response.text()).toBe(expected);
  });

  test("classifies pre-output stall, EOF, read failure, and incomplete terminal as retryable", async () => {
    const cases: Array<{
      name: string;
      response: () => Response;
      status: number;
    }> = [
      {
        name: "stall",
        response: () => new Response(new ReadableStream<Uint8Array>({
          pull() { return new Promise<void>(() => {}); },
        }), { headers: { "content-type": "text/event-stream" } }),
        status: 504,
      },
      {
        name: "EOF",
        response: () => new Response(new ReadableStream<Uint8Array>({
          start(controller) { controller.close(); },
        }), { headers: { "content-type": "text/event-stream" } }),
        status: 502,
      },
      {
        name: "read failure",
        response: () => new Response(new ReadableStream<Uint8Array>({
          pull() { throw new Error("transport failed"); },
        }), { headers: { "content-type": "text/event-stream" } }),
        status: 502,
      },
      {
        name: "incomplete terminal",
        response: () => sse(
          { type: "response.created", response: { id: "r1", status: "in_progress" } },
          {
            type: "response.incomplete",
            response: {
              id: "r1",
              status: "incomplete",
              error: { type: "server_error", code: "upstream_incomplete", message: "incomplete" },
            },
          },
        ),
        status: 502,
      },
    ];

    for (const testCase of cases) {
      const result = await preflightComboStreamResponse(
        testCase.response(),
        { model: "m1", provider: "a" },
        { stallMs: 10 },
      );
      expect(result.kind).toBe("failed");
      expect(result.response.status).toBe(testCase.status);
    }
  });

  test("client abort cancels a blocked preflight instead of selecting a backup", async () => {
    let cancels = 0;
    let started!: () => void;
    const reading = new Promise<void>(resolve => { started = resolve; });
    const response = new Response(new ReadableStream<Uint8Array>({
      pull() {
        started();
        return new Promise<void>(() => {});
      },
      cancel() { cancels += 1; },
    }), { headers: { "content-type": "text/event-stream" } });
    const abort = new AbortController();
    const pending = preflightComboStreamResponse(
      response,
      { model: "m1", provider: "a" },
      { abortSignal: abort.signal, stallMs: 1_000 },
    );
    await reading;
    const reason = new DOMException("client closed", "AbortError");
    abort.abort(reason);
    await expect(pending).rejects.toBe(reason);
    expect(cancels).toBe(1);
  });

  test("replays buffered bytes unchanged after output commits the target", async () => {
    const original = [
      { type: "response.created", response: { id: "r1", status: "in_progress" } },
      { type: "response.output_text.delta", delta: "visible" },
      {
        type: "response.failed",
        response: { status: "failed", error: { type: "server_error", message: "late" } },
      },
    ];
    const source = sse(...original);
    const expected = await source.clone().text();
    const result = await preflightComboStreamResponse(source, { model: "m1", provider: "a" });

    expect(result.kind).toBe("accepted");
    expect(await result.response.text()).toBe(expected);
  });

  test("canonical Anthropic replay stops at its first terminal frame", async () => {
    const stop = `data: ${JSON.stringify({ type: "message_stop" })}\n\n`;
    const error = `data: ${JSON.stringify({
      type: "error",
      error: { type: "overloaded_error", message: "busy" },
    })}\n\n`;

    const completed = await preflightComboStreamResponse(
      new Response(stop + error, { headers: { "content-type": "text/event-stream" } }),
      { model: "m1", provider: "a" },
      { expectedTransport: "sse" },
    );
    expect(completed.kind).toBe("accepted");
    expect(await completed.response.text()).toBe(stop);

    const failed = await preflightComboStreamResponse(
      new Response(error + stop, { headers: { "content-type": "text/event-stream" } }),
      { model: "m1", provider: "a" },
      { expectedTransport: "sse" },
    );
    expect(failed.kind).toBe("failed");
    expect(failed.response.status).toBe(529);
  });

  test("canonical Anthropic preflight rejects Responses terminal events", async () => {
    const result = await preflightComboStreamResponse(
      sse({ type: "response.completed", response: { status: "completed" } }),
      { model: "m1", provider: "a" },
      { expectedTransport: "sse" },
    );

    expect(result.kind).toBe("failed");
    expect(result.response.status).toBe(502);
  });

  test("bounds strict validated frames within one source chunk", async () => {
    const encoder = new TextEncoder();
    const ping = `data: ${JSON.stringify({
      type: "ping",
      padding: "x".repeat(2_048),
    })}\n\n`;
    const frameCount = Math.ceil(MAX_CLIENT_SSE_FRAME_BYTES / encoder.encode(ping).byteLength) + 1;
    const chunk = encoder.encode(ping.repeat(frameCount) + 'data: {"type":"message_stop"}\n\n');
    expect(chunk.byteLength).toBeGreaterThan(MAX_CLIENT_SSE_FRAME_BYTES);

    const result = await preflightComboStreamResponse(
      new Response(chunk, { headers: { "content-type": "text/event-stream" } }),
      { model: "m1", provider: "a" },
      { expectedTransport: "sse" },
    );

    expect(result.kind).toBe("accepted");
    expect(new Uint8Array(await result.response.arrayBuffer())).toEqual(chunk);
  });

  test("canonical Anthropic preflight rejects more than 4,096 frames in one chunk", async () => {
    const encoder = new TextEncoder();
    const ping = 'data: {"type":"ping"}\n\n';
    const stop = 'data: {"type":"message_stop"}\n\n';
    const chunk = encoder.encode(ping.repeat(4_097) + stop);
    const result = await preflightComboStreamResponse(
      new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(chunk);
          controller.close();
        },
      }), { headers: { "content-type": "text/event-stream" } }),
      { model: "m1", provider: "a" },
      { expectedTransport: "sse" },
    );

    expect(result.kind).toBe("failed");
    expect(result.response.status).toBe(502);
    expect(await result.response.json()).toMatchObject({
      error: {
        message: "Provider stream protocol error: upstream SSE chunk exceeded 4,096 complete frames",
      },
    });
  });

  test("commits a non-strict stream when one chunk exceeds the inspection frame limit", async () => {
    const heartbeat = 'data: {"type":"response.heartbeat"}\n\n';
    const output = 'data: {"type":"response.output_text.delta","delta":"x"}\n\n';
    const terminal = 'data: {"type":"response.completed"}\n\n';
    const body = heartbeat.repeat(4_097) + output + terminal;
    const chunk = new TextEncoder().encode(body);
    const result = await preflightComboStreamResponse(
      new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(chunk);
          controller.close();
        },
      }), { headers: { "content-type": "text/event-stream" } }),
      { model: "m1", provider: "a" },
    );

    expect(result.kind).toBe("accepted");
    expect(await result.response.text()).toBe(body);
  });

  test("commits an oversized next chunk without copying it beyond the preflight cap", async () => {
    const encoder = new TextEncoder();
    const preamble = encoder.encode(`data: ${JSON.stringify({
      type: "response.created",
      response: { id: "r1", status: "in_progress" },
    })}\n\n`);
    const oversized = new Uint8Array(MAX_CLIENT_SSE_FRAME_BYTES + 1);
    oversized.fill(120);
    const chunks = [preamble, oversized];
    const response = new Response(new ReadableStream<Uint8Array>({
      pull(controller) {
        const chunk = chunks.shift();
        if (chunk) controller.enqueue(chunk);
        else controller.close();
      },
    }), { headers: { "content-type": "text/event-stream" } });

    const result = await preflightComboStreamResponse(response, { model: "m1", provider: "a" });
    expect(result.kind).toBe("accepted");
    const reader = result.response.body!.getReader();
    const first = await reader.read();
    const second = await reader.read();
    expect(new TextDecoder().decode(first.value)).toBe(new TextDecoder().decode(preamble));
    expect(second.value).toBe(oversized);
    await reader.cancel();
  });

  test("commits at the retained-chunk boundary without reading one more chunk", async () => {
    const prefix = Array.from(
      { length: preflightChunkLimit },
      (_, index) => Uint8Array.of((index % 251) + 1),
    );
    const tail = Uint8Array.of(252, 253);
    let sourceIndex = 0;
    let releaseTail: (() => void) | undefined;
    let reportNextPull!: () => void;
    const nextPull = new Promise<void>(resolve => { reportNextPull = resolve; });
    const response = new Response(new ReadableStream<Uint8Array>({
      pull(controller) {
        if (sourceIndex < prefix.length) {
          controller.enqueue(prefix[sourceIndex++]!);
          return;
        }
        reportNextPull();
        return new Promise<void>(resolve => {
          releaseTail = () => {
            controller.enqueue(tail);
            controller.close();
            resolve();
          };
        });
      },
    }, { highWaterMark: 0 }), { headers: { "content-type": "text/event-stream" } });

    const preflight = preflightComboStreamResponse(response, { model: "m1", provider: "a" });
    const winner = await Promise.race([
      preflight.then(result => ({ kind: "preflight" as const, result })),
      nextPull.then(() => ({ kind: "next-pull" as const })),
    ]);
    if (winner.kind === "next-pull") {
      releaseTail!();
      const late = await preflight;
      await late.response.body?.cancel();
    }
    expect(winner.kind).toBe("preflight");
    if (winner.kind !== "preflight") return;

    expect(winner.result.kind).toBe("accepted");
    const reader = winner.result.response.body!.getReader();
    for (let index = 0; index < prefix.length; index += 1) {
      const next = await reader.read();
      expect(next.done).toBe(false);
      expect(next.value).not.toBe(prefix[index]);
      expect(next.value).toEqual(prefix[index]);
    }

    const tailRead = reader.read();
    await nextPull;
    expect(releaseTail).toBeDefined();
    releaseTail!();
    const replayedTail = await tailRead;
    expect(replayedTail.done).toBe(false);
    expect(replayedTail.value).toBe(tail);
    expect((await reader.read()).done).toBe(true);
  });

  test("keeps a failed terminal authoritative at the retained-chunk boundary", async () => {
    const encoder = new TextEncoder();
    const comment = encoder.encode(":\n\n");
    const failed = encoder.encode(`data: ${JSON.stringify({
      type: "response.failed",
      response: {
        status: "failed",
        error: { type: "server_error", code: "upstream_server_error", message: "busy" },
        usage: { input_tokens: 9, output_tokens: 0, total_tokens: 9 },
        provider_trace_id: "must-not-cross-the-combo-boundary",
      },
    })}\n\n`);
    let sourceIndex = 0;
    const response = new Response(new ReadableStream<Uint8Array>({
      pull(controller) {
        if (sourceIndex < preflightChunkLimit - 1) {
          sourceIndex += 1;
          controller.enqueue(comment);
          return;
        }
        if (sourceIndex === preflightChunkLimit - 1) {
          sourceIndex += 1;
          controller.enqueue(failed);
        }
      },
    }, { highWaterMark: 0 }), { headers: { "content-type": "text/event-stream" } });

    const result = await preflightComboStreamResponse(response, { model: "m1", provider: "a" });
    expect(result.kind).toBe("failed");
    expect(result.response.status).toBe(502);
    const body = await result.response.json();
    expect(body).toMatchObject({
      error: { code: "upstream_server_error", message: "busy" },
      response: { usage: { input_tokens: 9, output_tokens: 0 } },
    });
    expect(JSON.stringify(body)).not.toContain("provider_trace_id");
  });

  const DECRYPT_REJECTION =
    "Encrypted function output content could not be decrypted or decoded.";

  const exactDecryptRetryable = (payload: unknown): boolean => {
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) return false;
    const event = payload as {
      type?: unknown;
      message?: unknown;
      error?: { message?: unknown };
      response?: { error?: { message?: unknown } };
    };
    if (event.type !== "error" && event.type !== "response.failed" && event.type !== "response.incomplete") {
      return false;
    }
    const message = event.error?.message
      ?? event.response?.error?.message
      ?? (event.type === "error" ? event.message : undefined);
    return message === DECRYPT_REJECTION;
  };

  test("default preflight retries zero-output bare errors without structured status", async () => {
    expect(comboStreamPayloadCommitsOutput({ type: "error" })).toBe(true);
    for (const [payload, status] of [
      [{ type: "error", message: "An error occurred while processing your request. Please include request ID r1." }, 502],
      [{ type: "error", error: { message: "unknown upstream failure" } }, 502],
      [{ type: "error", error: { type: "server_error", message: "busy" } }, 502],
      [{ type: "error", error: { status: "429", message: "slow down" } }, 429],
      [{ type: "error", error: { http_status: 503, message: "unavailable" } }, 503],
    ]) {
      const source = sse(
        { type: "response.created", response: { id: "r1", status: "in_progress" } },
        payload,
      );
      const result = await preflightComboStreamResponse(source, { model: "m1", provider: "a" });
      expect(result.kind).toBe("failed");
      expect(result.response.status).toBe(status);
    }
  });

  test("does not retry explicit zero-output client errors", async () => {
    for (const payload of [
      { type: "error", error: { status: 400, message: "bad request" } },
      { type: "error", error: { type: "invalid_request_error", message: "bad parameter" } },
      { type: "error", code: "invalid_request_error", message: "bad argument" },
    ]) {
      const source = sse(
        { type: "response.created", response: { id: "r1", status: "in_progress" } },
        payload,
      );
      const expected = await source.clone().text();
      const result = await preflightComboStreamResponse(source, { model: "m1", provider: "a" });
      expect(result.kind).toBe("accepted");
      expect(await result.response.text()).toBe(expected);
    }
  });

  test("passes a zero-output credential error to the ordinary combo classifier", async () => {
    const result = await preflightComboStreamResponse(sse({
      type: "error",
      error: { type: "authentication_error", message: "bad credential" },
    }), { model: "m1", provider: "a" });

    expect(result.kind).toBe("failed");
    expect(result.response.status).toBe(401);
  });

  test("retries a structured model-lifecycle 410 through the ordinary combo classifier", async () => {
    const result = await preflightComboStreamResponse(sse(
      { type: "response.created", response: { id: "r1", status: "in_progress" } },
      {
        type: "error",
        status: 410,
        error: { code: "model_end_of_life", message: "model retired" },
      },
    ), { model: "m1", provider: "a" });

    expect(result.kind).toBe("failed");
    expect(result.response.status).toBe(410);
  });

  test("honors root status when a bare error has a nested error object", async () => {
    const clientError = sse({
      type: "error",
      status: 400,
      error: { status: 503, message: "bad request" },
    });
    const expected = await clientError.clone().text();
    const clientResult = await preflightComboStreamResponse(clientError, { model: "m1", provider: "a" });
    expect(clientResult.kind).toBe("accepted");
    expect(await clientResult.response.text()).toBe(expected);

    const serverResult = await preflightComboStreamResponse(sse({
      type: "error",
      status: 503,
      error: { status: 400, message: "upstream failed" },
    }), { model: "m1", provider: "a" });
    expect(serverResult.kind).toBe("failed");
    expect(serverResult.response.status).toBe(503);
  });

  test("treats invalid explicit error statuses as unknown upstream failures", async () => {
    for (const status of [Number.NaN, 204, 999, "999"]) {
      const result = await preflightComboStreamResponse(sse({
        type: "error",
        status,
        error: { message: "upstream failed" },
      }), { model: "m1", provider: "a" });
      expect(result.kind).toBe("failed");
      expect(result.response.status).toBe(502);
    }
  });

  test("an explicit predicate can retry a known client-classified bare error", async () => {
    const source = sse(
      { type: "response.created", response: { id: "r1", status: "in_progress" } },
      { type: "error", error: { status: 400, message: DECRYPT_REJECTION } },
    );
    const original = await source.clone().text();
    const result = await preflightComboStreamResponse(
      source,
      { model: "m1", provider: "a" },
      exactDecryptRetryable,
    );

    expect(result.kind).toBe("failed");
    expect(result.response.status).toBe(400);
    expect(result.response.headers.get("content-type")).toContain("application/json");
    expect(await result.response.text()).not.toBe(original);
  });

  test("an explicit predicate still commits an unrelated bare error", async () => {
    const source = sse(
      { type: "response.created", response: { id: "r1", status: "in_progress" } },
      { type: "error", message: "unrelated upstream busy" },
      {
        type: "response.failed",
        response: {
          status: "failed",
          error: { type: "server_error", message: DECRYPT_REJECTION },
        },
      },
    );
    const expected = await source.clone().text();
    const result = await preflightComboStreamResponse(
      source,
      { model: "m1", provider: "a" },
      exactDecryptRetryable,
    );

    expect(result.kind).toBe("accepted");
    expect(await result.response.text()).toBe(expected);
  });

  test("output before a bare error does not retry", async () => {
    const source = sse(
      { type: "response.created", response: { id: "r1", status: "in_progress" } },
      { type: "response.output_text.delta", delta: "visible" },
      { type: "error", message: DECRYPT_REJECTION },
    );
    const expected = await source.clone().text();
    const result = await preflightComboStreamResponse(
      source,
      { model: "m1", provider: "a" },
      exactDecryptRetryable,
    );

    expect(result.kind).toBe("accepted");
    expect(await result.response.text()).toBe(expected);
  });

  test("a bare error before output in the same chunk keeps the retry decision", async () => {
    const result = await preflightComboStreamResponse(sse(
      { type: "error", message: "upstream failed" },
      { type: "response.output_text.delta", delta: "too late" },
    ), { model: "m1", provider: "a" });

    expect(result.kind).toBe("failed");
    expect(result.response.status).toBe(502);
  });

  test("a completed terminal before a bare error in the same chunk stays authoritative", async () => {
    const source = sse(
      { type: "response.completed", response: { id: "r1", status: "completed", output: [] } },
      { type: "error", message: "too late" },
    );
    const expected = await source.clone().text();
    const result = await preflightComboStreamResponse(source, { model: "m1", provider: "a" });

    expect(result.kind).toBe("accepted");
    expect(await result.response.text()).toBe(expected);
  });

  test("default missing content-type is refused, and allowMissingContentType accepts only an absent type", async () => {
    const payloads = [
      { type: "response.created", response: { id: "r1", status: "in_progress" } },
      { type: "error", message: DECRYPT_REJECTION },
    ];
    const body = payloads.map(payload => "data: " + JSON.stringify(payload) + "\n\n").join("");
    const encoded = () => new TextEncoder().encode(body);
    const missingTypeResponse = () => {
      const headers = new Headers();
      headers.delete("content-type");
      const response = new Response(encoded(), { headers });
      response.headers.delete("content-type");
      return response;
    };

    const missing = missingTypeResponse();
    expect(missing.headers.get("content-type")).toBeNull();
    const missingDefault = await preflightComboStreamResponse(missing, { model: "m1", provider: "a" });
    expect(missingDefault.kind).toBe("accepted");
    expect(await missingDefault.response.text()).toBe(body);

    const allowedMissingSource = missingTypeResponse();
    expect(allowedMissingSource.headers.get("content-type")).toBeNull();
    const allowedMissing = await preflightComboStreamResponse(
      allowedMissingSource,
      { model: "m1", provider: "a" },
      exactDecryptRetryable,
      { allowMissingContentType: true },
    );
    expect(allowedMissing.kind).toBe("failed");
    expect(allowedMissing.response.status).toBe(502);

    for (const contentType of ["application/json", "text/plain"]) {
      const source = new Response(encoded(), { headers: { "content-type": contentType } });
      const result = await preflightComboStreamResponse(
        source,
        { model: "m1", provider: "a" },
        exactDecryptRetryable,
        { allowMissingContentType: true },
      );
      expect(result.kind).toBe("accepted");
      expect(await result.response.text()).toBe(body);
    }
  });

  test("default reader.read rejection still throws and does not cancel the reader", async () => {
    const readError = new Error("preflight-read-reset");
    const source = prefixThenReadError(createdPrefix, readError);
    await expect(preflightComboStreamResponse(source.response, { model: "m1", provider: "a" }))
      .rejects.toBe(readError);
    expect(source.cancelSpy()).toBeDefined();
    expect(source.cancelSpy()!.mock.calls).toHaveLength(0);
  });

  test("replayReadErrors returns a reconstructed prefix, the same read error, and the observed stage", async () => {
    const readError = new Error("preflight-read-reset");
    const source = prefixThenReadError(createdPrefix, readError);
    const result = await preflightComboStreamResponse(
      source.response,
      { model: "m1", provider: "a" },
      undefined,
      { replayReadErrors: true },
    );
    expect(result.kind).toBe("read-error");
    if (result.kind === "read-error") {
      expect(result.error).toBe(readError);
      // response.created and nothing else: the failure model puts that in the prelude, and a
      // prelude is a stage at which the caller has observed nothing.
      expect(result.stage).toBe("protocol-prelude");
      expect(stageCommitment(result.stage)).toBe("nothing-observed");
    }
    expect(source.cancelSpy()).toBeDefined();
    expect(source.cancelSpy()!.mock.calls).toHaveLength(0);
    const reader = result.response.body!.getReader();
    const first = await reader.read();
    expect(first.done).toBe(false);
    expect(first.value).toEqual(createdPrefix);
    await expect(reader.read()).rejects.toBe(readError);
    expect(source.cancelSpy()).toBeDefined();
    expect(source.cancelSpy()!.mock.calls).toHaveLength(0);
  });

  test("a read error before any event is headers-only, and a committed stream never reports one", async () => {
    const readError = new Error("preflight-read-reset");
    const bare = await preflightComboStreamResponse(
      prefixThenReadError(new TextEncoder().encode(""), readError).response,
      { model: "m1", provider: "a" },
      undefined,
      { replayReadErrors: true },
    );
    expect(bare.kind).toBe("read-error");
    if (bare.kind === "read-error") {
      expect(bare.stage).toBe("headers-only");
      expect(stageCommitment(bare.stage)).toBe("nothing-observed");
    }

    // Once output commits the preflight stops buffering and hands the body back, so the read
    // error that follows happens on the caller's side of the boundary and no stage is ever
    // reported. That is the stronger statement: a committed stream does not reach the resend
    // gate at all, rather than reaching it and being refused there.
    const outputPrefix = new TextEncoder().encode(`data: ${JSON.stringify({
      type: "response.output_text.delta", delta: "hi",
    })}\n\n`);
    const committed = await preflightComboStreamResponse(
      prefixThenReadError(outputPrefix, readError).response,
      { model: "m1", provider: "a" },
      undefined,
      { replayReadErrors: true },
    );
    expect(committed.kind).toBe("accepted");
    // The prefix is still relayed and the error still reaches whoever reads it.
    const reader = committed.response.body!.getReader();
    expect((await reader.read()).value).toEqual(outputPrefix);
    await expect(reader.read()).rejects.toBe(readError);
  });

  /**
   * The boundary that decides resend permission, asserted where it is actually enforced.
   *
   * The stage a read error is reported at is only half the guarantee. What matters is that a
   * stream which committed output never gets a replacement offered at all, and the seam that
   * decides it is the deferred wrapper, not the preflight. The commitment is read from
   * `stageCommitment` rather than compared against a written-out stage name, so a stage added
   * to the model later cannot pass this by being unlisted.
   */
  test("a replacement is offered only for a stage the caller observed nothing at", async () => {
    const readError = new Error("preflight-read-reset");
    const logCtx: RequestLogContext = { model: "m1", provider: "a" };
    const seen: RequestFailureStage[] = [];
    const recover = async (_error: unknown, stage: RequestFailureStage): Promise<Response | null> => {
      seen.push(stage);
      return null;
    };

    const prelude = deferProtocolSafeResetRecovery(
      prefixThenReadError(createdPrefix, readError).response, logCtx, recover);
    const preludeReader = prelude.body!.getReader();
    expect((await preludeReader.read()).value).toEqual(createdPrefix);
    await expect(preludeReader.read()).rejects.toBe(readError);
    expect(seen).toHaveLength(1);
    expect(stageCommitment(seen[0]!)).toBe("nothing-observed");

    seen.length = 0;
    const outputPrefix = new TextEncoder().encode(`data: ${JSON.stringify({
      type: "response.output_text.delta", delta: "hi",
    })}\n\n`);
    const committed = deferProtocolSafeResetRecovery(
      prefixThenReadError(outputPrefix, readError).response, logCtx, recover);
    const committedReader = committed.body!.getReader();
    expect((await committedReader.read()).value).toEqual(outputPrefix);
    await expect(committedReader.read()).rejects.toBe(readError);
    // Never consulted. A turn whose output the caller already saw cannot be replaced, and it
    // does not get as far as asking.
    expect(seen).toEqual([]);
  });

  test("a response.created carrying output is not a prelude", () => {
    expect(comboStreamPayloadCommitsOutput({
      type: "response.created", response: { id: "r1", output: [] },
    })).toBe(false);
    expect(comboStreamPayloadCommitsOutput({
      type: "response.created",
      response: { id: "r1", output: [{ type: "message", role: "assistant" }] },
    })).toBe(true);
  });

  test("preserves nested Anthropic server-tool usage with mixed-case SSE MIME", async () => {
    const logCtx: RequestLogContext = { model: "m1", provider: "a" };
    const body = [
      'data: {"type":"message_start","message":{"usage":{"input_tokens":3,"server_tool_use":{"web_search_requests":2}}}}\n\n',
      'data: {"type":"message_delta","usage":{"output_tokens":1,"server_tool_use":{"web_search_requests":2}}}\n\n',
      'data: {"type":"message_stop"}\n\n',
    ].join("");
    const result = await preflightComboStreamResponse(
      new Response(body, { headers: { "content-type": "Text/Event-Stream; Charset=UTF-8" } }),
      logCtx,
      { expectedTransport: "sse" },
    );

    expect(result.kind).toBe("accepted");
    expect(await result.response.text()).toBe(body);
    expect(logCtx.usage).toMatchObject({
      inputTokens: 3,
      outputTokens: 1,
      anthropicServerToolUse: { web_search_requests: 2 },
    });
  });

  test("returns without awaiting a non-settling source cancellation", async () => {
    const response = new Response(new ReadableStream<Uint8Array>({
      cancel() { return new Promise<void>(() => {}); },
    }), { headers: { "content-type": "application/json" } });

    const result = await Promise.race([
      preflightComboStreamResponse(
        response,
        { model: "m1", provider: "a" },
        { expectedTransport: "sse" },
      ),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("preflight hung on cancel")), 100)),
    ]);
    expect(result.kind).toBe("failed");
    expect(result.response.status).toBe(502);
  });

  test("empty source chunks do not renew the preflight inactivity deadline", async () => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const response = new Response(new ReadableStream<Uint8Array>({
      pull(controller) {
        return new Promise<void>(resolve => {
          timer = setTimeout(() => {
            controller.enqueue(new Uint8Array(0));
            resolve();
          }, 1);
        });
      },
      cancel() {
        if (timer) clearTimeout(timer);
      },
    }, { highWaterMark: 0 }), { headers: { "content-type": "text/event-stream" } });

    const result = await Promise.race([
      preflightComboStreamResponse(
        response,
        { model: "m1", provider: "a" },
        { expectedTransport: "sse", stallMs: 15 },
      ),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("empty chunks prevented timeout")), 200)),
    ]);
    expect(result.kind).toBe("failed");
    expect(result.response.status).toBe(504);
  });

  test("normalizes direct JSON terminal status while preserving exact source bytes", async () => {
    const bytes = new TextEncoder().encode(
      ' {"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}\n',
    );
    const result = await preflightComboStreamResponse(
      new Response(bytes, {
        status: 200,
        statusText: "OK",
        headers: { "content-type": "application/json", "retry-after": "17" },
      }),
      { model: "m1", provider: "a" },
      { expectedTransport: "json" },
    );

    expect(result.kind).toBe("failed");
    expect(result.response.status).toBe(429);
    expect(result.passthroughResponse?.status).toBe(429);
    expect(result.passthroughResponse?.statusText).toBe("");
    expect(result.passthroughResponse?.headers.get("retry-after")).toBe("17");
    expect(new Uint8Array(await result.passthroughResponse!.arrayBuffer())).toEqual(bytes);
  });

  test("maps structured Anthropic root errors to semantic HTTP statuses", async () => {
    for (const [type, status] of [
      ["authentication_error", 401],
      ["permission_error", 403],
      ["invalid_request_error", 400],
      ["rate_limit_error", 429],
      ["overloaded_error", 529],
    ] as const) {
      const result = await preflightComboStreamResponse(
        sse({ type: "error", error: { type, message: "structured failure" } }),
        { model: "m1", provider: "a" },
        { expectedTransport: "sse" },
      );
      expect(result.kind).toBe("failed");
      expect(result.response.status).toBe(status);
    }
  });

  test("replays strict overflow bytes exactly when a frame crosses source chunks", async () => {
    const encoder = new TextEncoder();
    const ping = `data: ${JSON.stringify({ type: "ping", padding: "x".repeat(2048) })}\n\n`;
    const prefix = encoder.encode(ping.repeat(Math.floor(MAX_CLIENT_SSE_FRAME_BYTES / encoder.encode(ping).byteLength)));
    const crossing = encoder.encode(`${ping}data: {"type":"message_stop"}\n\n`);
    const split = Math.floor(crossing.byteLength / 2);
    const chunks = [prefix, crossing.subarray(0, split), crossing.subarray(split)];
    const expected = new Uint8Array(prefix.byteLength + crossing.byteLength);
    expected.set(prefix);
    expected.set(crossing, prefix.byteLength);
    const response = new Response(new ReadableStream<Uint8Array>({
      pull(controller) {
        const chunk = chunks.shift();
        if (chunk) controller.enqueue(chunk);
        else controller.close();
      },
    }, { highWaterMark: 0 }), { headers: { "content-type": "text/event-stream" } });

    const result = await preflightComboStreamResponse(
      response,
      { model: "m1", provider: "a" },
      { expectedTransport: "sse" },
    );
    expect(result.kind).toBe("accepted");
    expect(new Uint8Array(await result.response.arrayBuffer())).toEqual(expected);
  });
});
