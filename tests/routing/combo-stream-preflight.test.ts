import { describe, expect, test } from "bun:test";
import {
  comboStreamPayloadCommitsOutput,
  preflightComboStreamResponse,
} from "../../src/server/responses/combo-stream-preflight";
import type { RequestLogContext } from "../../src/server/request-log";
import { MAX_CLIENT_SSE_FRAME_BYTES } from "../../src/server/sse-frame-buffer";

const sse = (...payloads: unknown[]): Response => new Response(
  payloads.map(payload => `data: ${JSON.stringify(payload)}\n\n`).join(""),
  { headers: { "content-type": "text/event-stream" } },
);

const preflightChunkLimit = Math.max(1, Math.ceil(MAX_CLIENT_SSE_FRAME_BYTES / 1024));

describe("combo stream preflight", () => {
  test("keeps only lifecycle preamble replayable and treats unknown output conservatively", () => {
    expect(comboStreamPayloadCommitsOutput({ type: "response.created" })).toBe(false);
    expect(comboStreamPayloadCommitsOutput({ type: "response.heartbeat" })).toBe(false);
    expect(comboStreamPayloadCommitsOutput({ type: "response.failed" })).toBe(false);
    expect(comboStreamPayloadCommitsOutput({ type: "response.incomplete" })).toBe(false);
    expect(comboStreamPayloadCommitsOutput({ type: "error" })).toBe(false);
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

  test("converts a zero-output top-level error into a retryable HTTP failure", async () => {
    const logCtx: RequestLogContext = { model: "m1", provider: "a" };
    const result = await preflightComboStreamResponse(sse(
      { type: "response.created", response: { id: "r1", status: "in_progress" } },
      {
        type: "error",
        error: {
          type: "invalid_request_error",
          code: "unsupported_parameter",
          message: "Unsupported parameter: user",
        },
      },
    ), logCtx);

    expect(result.kind).toBe("failed");
    expect(result.response.status).toBe(400);
    const body = await result.response.json();
    expect(body).toEqual({
      error: {
        type: "invalid_request_error",
        code: "unsupported_parameter",
        message: "Unsupported parameter: user",
      },
      response: {
        error: {
          type: "invalid_request_error",
          code: "unsupported_parameter",
          message: "Unsupported parameter: user",
        },
      },
    });
    expect(logCtx.upstreamError).toBe("Unsupported parameter: user");
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
