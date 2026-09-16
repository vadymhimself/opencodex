import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAnthropicAdapter } from "../../src/adapters/anthropic";
import { clearComboTargetCooldowns, coolComboTarget } from "../../src/combos";
import { saveConfig } from "../../src/config";
import { clearPoolRotationState } from "../../src/codex/pool-rotation";
import { clearableDeadline } from "../../src/lib/abort";
import { estimateTokens } from "../../src/lib/token-estimate";
import { clearAnthropicAccountPoolState } from "../../src/oauth/anthropic-routing";
import {
  blockNativeMainRecovery,
  completeNativeMainRecovery,
  nativeMainStartupGateSnapshot,
  waitForNativeMainStartupGate,
} from "../../src/codex/native-profile-startup";
import { startServer } from "../../src/server";
import {
  estimateClaudeRequestTokens,
  fetchWithHeaderDeadline,
  handleClaudeCountTokens,
  handleClaudeMessages,
  passthroughBodyBufferGrowthsForTests,
  readBoundedPassthroughBody,
  readBoundedPassthroughBytes,
  resolvePassthroughBodyGuard,
  tapAnthropicSseForLog,
} from "../../src/server/claude-messages";
import {
  acquireNativeMainProfileDrain,
  activeRegistryMetrics,
  getNativeMainProfileRequestCount,
  resetLifecycleDrainStateForTests,
  tryAdmitTurn,
} from "../../src/server/lifecycle";
import {
  clearRequestLogsForTests,
  getRequestLogEntries,
  type RequestLogContext,
} from "../../src/server/request-log";
import type { OcxConfig, OcxParsedRequest } from "../../src/types";
import { installIsolatedCodexHome, type IsolatedCodexHome } from "../helpers/isolated-codex-home";
import { logsFromApiBody } from "../helpers/logs-api";
import { managementFetch as fetch } from "../helpers/management-auth";
import { ownedServiceHomeInspection } from "../helpers/owned-service-home-inspection";
import { removeTreeWithRetry } from "../helpers/remove-tree";
import { SERVER_BUDGET_MS } from "../helpers/test-budget";
import { createTestTranslatorBudget } from "../helpers/translator-budget";

let testDir = "";
let previousHome: string | undefined;
let previousDesktopConfigDir: string | undefined;
let isolatedCodexHome: IsolatedCodexHome | null = null;
const originalFetch = globalThis.fetch;

beforeEach(() => {
  previousHome = process.env.OPENCODEX_HOME;
  isolatedCodexHome = installIsolatedCodexHome("ocx-claude-endpoint-");
  testDir = mkdtempSync(join(tmpdir(), "ocx-claude-endpoint-"));
  process.env.OPENCODEX_HOME = testDir;
  previousDesktopConfigDir = process.env.OPENCODEX_CLAUDE_DESKTOP_CONFIG_DIR;
  process.env.OPENCODEX_CLAUDE_DESKTOP_CONFIG_DIR = join(testDir, "claude-desktop");
  clearRequestLogsForTests();
  globalThis.fetch = originalFetch;
});

afterEach(() => {
  if (previousHome === undefined) delete process.env.OPENCODEX_HOME;
  else process.env.OPENCODEX_HOME = previousHome;
  if (previousDesktopConfigDir === undefined) delete process.env.OPENCODEX_CLAUDE_DESKTOP_CONFIG_DIR;
  else process.env.OPENCODEX_CLAUDE_DESKTOP_CONFIG_DIR = previousDesktopConfigDir;
  isolatedCodexHome?.restore();
  isolatedCodexHome = null;
  globalThis.fetch = originalFetch;
  if (testDir) removeTreeWithRetry(testDir);
});

function mockChatUpstream() {
  return mockChatUpstreamCapturing().server;
}

function mockChatUpstreamCapturing() {
  const captured: Array<Record<string, unknown>> = [];
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);
      if (!url.pathname.endsWith("/chat/completions")) {
        return Response.json({ error: { message: `unexpected path ${url.pathname}` } }, { status: 404 });
      }
      try { captured.push(await req.json() as Record<string, unknown>); } catch { /* keep streaming */ }
      const frames = [
        `data: ${JSON.stringify({ choices: [{ index: 0, delta: { role: "assistant", content: "Hello" } }] })}\n\n`,
        `data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: " from mock" } }] })}\n\n`,
        `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 12, completion_tokens: 3 } })}\n\n`,
        "data: [DONE]\n\n",
      ];
      return new Response(frames.join(""), { headers: { "Content-Type": "text/event-stream" } });
    },
  });
  return { server, captured };
}

function mockConfig(baseUrl: string, claudeCode?: OcxConfig["claudeCode"]): OcxConfig {
  return {
    port: 0,
    defaultProvider: "mock",
    providers: {
      mock: { adapter: "openai-chat", baseUrl, apiKey: "k", allowPrivateNetwork: true },
    },
    ...(claudeCode ? { claudeCode } : {}),
  } as OcxConfig;
}

test("POST /v1/messages?beta=true streams an Anthropic-shaped turn end to end", async () => {
  const upstream = mockChatUpstream();
  saveConfig(mockConfig(`${upstream.url.toString().replace(/\/$/, "")}/v1`));
  const server = startServer(0);
  try {
    const response = await fetch(new URL("/v1/messages?beta=true", server.url), {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": "placeholder",
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "mock/test-model",
        max_tokens: 128,
        stream: true,
        messages: [{ role: "user", content: "hi" }],
      }),
    });
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type") ?? "").toContain("text/event-stream");
    const text = await response.text();
    const names = [...text.matchAll(/^event: (.+)$/gm)].map(m => m[1]);
    expect(names[0]).toBe("message_start");
    expect(names).toContain("content_block_start");
    expect(names).toContain("content_block_delta");
    expect(names).toContain("content_block_stop");
    expect(names.at(-2)).toBe("message_delta");
    expect(names.at(-1)).toBe("message_stop");
    expect(text).toContain("\"text_delta\"");
    expect(text).toContain("Hello");
    expect(text).toContain("\"stop_reason\":\"end_turn\"");

    // Request log regression (live smoke round 2): the tap must see the PRE-translation
    // Responses stream — the translated Anthropic stream has no response.completed, which
    // used to record a bogus 502 with no usage.
    const logs = logsFromApiBody<{
      status: number; model: string; usage?: { inputTokens: number; outputTokens: number }; usageStatus: string;
    }>(await (await fetch(new URL("/api/logs", server.url))).json());
    const row = logs.find(l => l.model === "test-model" || l.model === "mock/test-model");
    expect(row).toBeDefined();
    expect(row!.status).toBe(200);
    expect(row!.usage?.inputTokens).toBe(12);
    expect(row!.usage?.outputTokens).toBe(3);

    const claudeUsage = await fetch(new URL("/api/usage?range=all&surface=claude", server.url)).then(res => res.json()) as {
      surface: string;
      summary: { requests: number; totalTokens: number };
      models: Array<{ model: string }>;
    };
    expect(claudeUsage.surface).toBe("claude");
    expect(claudeUsage.summary).toMatchObject({ requests: 1, totalTokens: 15 });
    expect(claudeUsage.models).toEqual([expect.objectContaining({ model: "test-model" })]);

    const codexUsage = await fetch(new URL("/api/usage?range=all&surface=codex", server.url)).then(res => res.json()) as {
      surface: string;
      summary: { requests: number };
    };
    expect(codexUsage.surface).toBe("codex");
    expect(codexUsage.summary.requests).toBe(0);
  } finally {
    await server.stop(true);
    upstream.stop(true);
  }
}, { timeout: SERVER_BUDGET_MS });

test("non-streaming /v1/messages returns an Anthropic message JSON", async () => {
  const upstream = mockChatUpstream();
  saveConfig(mockConfig(`${upstream.url.toString().replace(/\/$/, "")}/v1`));
  const server = startServer(0);
  try {
    const response = await fetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "mock/test-model",
        max_tokens: 128,
        messages: [{ role: "user", content: "hi" }],
      }),
    });
    expect(response.status).toBe(200);
    const json = await response.json() as Record<string, any>;
    expect(json.type).toBe("message");
    expect(json.role).toBe("assistant");
    expect(json.model).toBe("mock/test-model");
    expect(json.stop_reason).toBe("end_turn");
    expect(json.content[0].type).toBe("text");
    expect(json.content[0].text).toContain("Hello");
    expect(typeof json.usage.input_tokens).toBe("number");
  } finally {
    await server.stop(true);
    upstream.stop(true);
  }
});

test("Desktop OFF leaves Claude messages and health live", async () => {
  const upstream = mockChatUpstream();
  saveConfig(mockConfig(`${upstream.url.toString().replace(/\/$/, "")}/v1`));
  const server = startServer(0);
  try {
    const disabled = await fetch(new URL("/api/native-integrations/claude-desktop", server.url), {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ enabled: false }),
    });
    expect(disabled.status).toBe(200);
    expect((await disabled.json()) as { desiredEnabled: boolean }).toMatchObject({ desiredEnabled: false });

    const message = await fetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "mock/test-model",
        max_tokens: 16,
        messages: [{ role: "user", content: "still live" }],
      }),
    });
    expect(message.status).toBe(200);
    expect((await message.json()) as { type: string }).toMatchObject({ type: "message" });
    expect((await fetch(new URL("/healthz", server.url))).status).toBe(200);
  } finally {
    await server.stop(true);
    upstream.stop(true);
  }
});

test("native generated-agent passthrough preserves legacy thinking", async () => {
  let captured: Record<string, unknown> | null = null;
  const upstream = Bun.serve({
    port: 0,
    async fetch(req) {
      captured = await req.json() as Record<string, unknown>;
      return Response.json({
        id: "msg_test",
        type: "message",
        role: "assistant",
        model: "claude-haiku-4-5",
        content: [{ type: "text", text: "ok" }],
        stop_reason: "end_turn",
        stop_sequence: null,
        usage: { input_tokens: 1, output_tokens: 1 },
      });
    },
  });
  const config = mockConfig("http://127.0.0.1:1/v1", {
    anthropicBaseUrl: upstream.url.toString().replace(/\/$/, ""),
  });
  saveConfig(config);
  const server = startServer(0);
  try {
    const response = await fetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "sk-ant-test" },
      body: JSON.stringify({
        model: "claude-haiku-4-5",
        max_tokens: 16,
        system: [
          { type: "text", text: "<!-- ocx-route: claude-haiku-4-5 -->" },
          { type: "text", text: "<!-- ocx-effort: max -->" },
        ],
        thinking: { type: "enabled", budget_tokens: 31999 },
        messages: [{ role: "user", content: "hi" }],
      }),
    });
    expect(response.status).toBe(200);
    await response.text();
    expect(captured).toMatchObject({
      model: "claude-haiku-4-5",
      thinking: { type: "enabled", budget_tokens: 31999 },
    });
    expect(captured).not.toHaveProperty("output_config");
  } finally {
    await server.stop(true);
    upstream.stop(true);
  }
});

test("native Anthropic passthrough clears the header deadline before streaming the body", async () => {
  const encoder = new TextEncoder();
  const upstream = Bun.serve({
    port: 0,
    fetch() {
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(encoder.encode('event: message_start\ndata: {"type":"message_start"}\n\n'));
          setTimeout(() => {
            controller.enqueue(encoder.encode('event: message_stop\ndata: {"type":"message_stop"}\n\n'));
            controller.close();
          }, 600);
        },
      });
      return new Response(body, { headers: { "content-type": "text/event-stream" } });
    },
  });
  const config = mockConfig("http://127.0.0.1:1/v1", {
    anthropicBaseUrl: upstream.url.toString().replace(/\/$/, ""),
  });
  config.connectTimeoutMs = 200;
  saveConfig(config);
  const server = startServer(0);
  try {
    const response = await fetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "sk-ant-test" },
      body: JSON.stringify({
        model: "claude-test",
        max_tokens: 16,
        stream: true,
        messages: [{ role: "user", content: "hi" }],
      }),
    });
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("message_stop");
  } finally {
    await server.stop(true);
    upstream.stop(true);
  }
});

// --- PR #136 follow-up hardening: deadline cleanup is guaranteed on EVERY fetch path ---

function spyDeadlineFactory() {
  const calls = { made: 0, clear: 0 };
  const factory: typeof clearableDeadline = (timeoutMs, parent) => {
    calls.made += 1;
    const real = clearableDeadline(timeoutMs, parent);
    return {
      ...real,
      clear: () => {
        calls.clear += 1;
        real.clear();
      },
    };
  };
  return { factory, calls };
}

test("fetchWithHeaderDeadline clears the deadline exactly once on the success path", async () => {
  const { factory, calls } = spyDeadlineFactory();
  const fetchImpl = (async () => new Response("ok")) as unknown as typeof fetch;
  const result = await fetchWithHeaderDeadline("http://127.0.0.1:1/x", {}, 60_000, undefined, factory, fetchImpl);
  expect(result.kind).toBe("response");
  expect(calls.made).toBe(1);
  expect(calls.clear).toBe(1);
});

test("fetchWithHeaderDeadline clears the deadline exactly once when fetch rejects (timer-leak regression)", async () => {
  const { factory, calls } = spyDeadlineFactory();
  const fetchImpl = (async () => {
    throw new Error("connection refused");
  }) as unknown as typeof fetch;
  const result = await fetchWithHeaderDeadline("http://127.0.0.1:1/x", {}, 60_000, undefined, factory, fetchImpl);
  expect(result.kind).toBe("error");
  expect(calls.made).toBe(1);
  expect(calls.clear).toBe(1);
});

test("fetchWithHeaderDeadline classifies expiry as timeout and still clears exactly once", async () => {
  const { factory, calls } = spyDeadlineFactory();
  const fetchImpl = ((_input: unknown, init?: RequestInit) =>
    new Promise((_resolve, reject) => {
      init?.signal?.addEventListener("abort", () => reject(init.signal!.reason), { once: true });
    })) as unknown as typeof fetch;
  const result = await fetchWithHeaderDeadline("http://127.0.0.1:1/x", {}, 10, undefined, factory, fetchImpl);
  expect(result.kind).toBe("timeout");
  expect(calls.made).toBe(1);
  expect(calls.clear).toBe(1);
});

test("native Anthropic passthrough returns 502 when the upstream connection is refused (reject-path activation)", async () => {
  const closed = Bun.serve({ port: 0, fetch: () => new Response() });
  const closedOrigin = closed.url.toString().replace(/\/$/, "");
  closed.stop(true);
  const config = mockConfig("http://127.0.0.1:1/v1", {
    anthropicBaseUrl: closedOrigin,
  });
  config.connectTimeoutMs = 60_000;
  saveConfig(config);
  const server = startServer(0);
  try {
    const response = await fetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "sk-ant-test" },
      body: JSON.stringify({
        model: "claude-test",
        max_tokens: 16,
        messages: [{ role: "user", content: "hi" }],
      }),
    });
    expect(response.status).toBe(502);
    const json = await response.json() as Record<string, any>;
    expect(json.error?.type).toBe("api_error");
    expect(String(json.error?.message)).toContain("anthropic passthrough failed");
  } finally {
    await server.stop(true);
  }
});

// --- Body-occupancy guard (devlog 260716_passthrough_followups/010): idle + size, never total-wall-clock ---

const sseEncoder = new TextEncoder();

function spyFinalize() {
  const calls: Array<{ status: number; closeReason: string }> = [];
  return {
    calls,
    finalize: (status: number, meta: { closeReason: string }) => calls.push({ status, closeReason: meta.closeReason }),
  };
}

function freshLogCtx(): RequestLogContext {
  return { model: "claude-test", provider: "anthropic-native" };
}

const MESSAGE_START_FRAME = 'event: message_start\ndata: {"type":"message_start","message":{"usage":{"input_tokens":3}}}\n\n';

// #1170: the space after `data:` is optional in text/event-stream. This tap extracted usage with
// a hardcoded `data: ` prefix, so a compliant provider that omits the space produced a logged turn
// with no usage at all.
const UNSPACED_USAGE_FRAMES = [
  'event:message_start\ndata:{"type":"message_start","message":{"usage":{"input_tokens":11}}}\n\n',
  'event:message_delta\ndata:{"type":"message_delta","usage":{"output_tokens":7}}\n\n',
  'event:message_stop\ndata:{"type":"message_stop"}\n\n',
].join("");
const MESSAGE_STOP_FRAME = 'event:message_stop\ndata:{"type":"message_stop"}\n\n';
const ROOT_ERROR_FRAME = 'event:error\ndata:{"type":"error","error":{"type":"overloaded_error","message":"busy"}}\n\n';

test("A0: usage extraction accepts unspaced data fields (#1170)", async () => {
  const upstream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(sseEncoder.encode(UNSPACED_USAGE_FRAMES));
      controller.close();
    },
  });
  const { calls, finalize } = spyFinalize();
  const ctx = freshLogCtx();
  const tap = tapAnthropicSseForLog(upstream, ctx, finalize, { stallMs: 5_000, maxBytes: 0 });
  const text = await new Response(tap).text();

  // The bytes pass through untouched either way; what the strict prefix broke was the inspection.
  expect(text).toContain("message_start");
  // `message_stop` is the only successful Anthropic terminal.
  expect(calls).toEqual([{ status: 200, closeReason: "terminal" }]);
  expect(ctx.usage).toEqual(expect.objectContaining({ inputTokens: 11, outputTokens: 7 }));
});

test("A0b: truncated Anthropic SSE preserves prior bytes, records inclusive usage, and appends adapter_eof", async () => {
  const prefix = [
    'event: message_start\ndata: {"type":"message_start","message":{"usage":{"input_tokens":3,"cache_creation_input_tokens":5,"cache_read_input_tokens":7}}}\n\n',
    'event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n',
  ].join("");
  const upstream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(sseEncoder.encode(prefix));
      controller.close();
    },
  });
  const ctx = freshLogCtx();
  const calls: Array<{ status: number; meta: unknown }> = [];
  let firstOutputs = 0;
  const tap = tapAnthropicSseForLog(
    upstream,
    ctx,
    (status, meta) => calls.push({ status, meta }),
    { stallMs: 5_000, maxBytes: 0 },
    () => { firstOutputs++; },
  );
  const text = await new Response(tap).text();

  expect(text.startsWith(prefix)).toBe(true);
  expect(text).toContain("upstream response was incomplete (adapter_eof)");
  expect(ctx.usage).toEqual({
    inputTokens: 15,
    outputTokens: 0,
    cachedInputTokens: 7,
    cacheReadInputTokens: 7,
    cacheCreationInputTokens: 5,
  });
  expect(firstOutputs).toBe(1);
  expect(calls).toEqual([{
    status: 502,
    meta: { closeReason: "terminal", terminalStatus: "incomplete" },
  }]);
});

test("A0c: Anthropic root error is relayed unchanged and recorded failed", async () => {
  const frame = 'event:error\ndata:{"type":"error","error":{"type":"overloaded_error","message":"busy"}}\n\n';
  const upstream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(sseEncoder.encode(frame));
      controller.close();
    },
  });
  const calls: Array<{ status: number; meta: unknown }> = [];
  const body = await new Response(tapAnthropicSseForLog(
    upstream,
    freshLogCtx(),
    (status, meta) => calls.push({ status, meta }),
    { stallMs: 5_000, maxBytes: 0 },
  )).text();

  expect(body).toBe(frame);
  expect(calls).toEqual([{
    status: 502,
    meta: { closeReason: "terminal", terminalStatus: "failed" },
  }]);
});

test.each([
  {
    name: "message_stop then error in one read",
    chunks: [MESSAGE_STOP_FRAME + ROOT_ERROR_FRAME],
    expectedBody: MESSAGE_STOP_FRAME,
    expectedStatus: 200,
    expectedTerminal: "completed",
  },
  {
    name: "message_stop then error across reads",
    chunks: [MESSAGE_STOP_FRAME, ROOT_ERROR_FRAME],
    expectedBody: MESSAGE_STOP_FRAME,
    expectedStatus: 200,
    expectedTerminal: "completed",
  },
  {
    name: "error then message_stop in one read",
    chunks: [ROOT_ERROR_FRAME + MESSAGE_STOP_FRAME],
    expectedBody: ROOT_ERROR_FRAME,
    expectedStatus: 502,
    expectedTerminal: "failed",
  },
])("A0c terminal boundary: $name", async ({
  chunks,
  expectedBody,
  expectedStatus,
  expectedTerminal,
}) => {
  const pending = [...chunks];
  const upstream = new ReadableStream<Uint8Array>({
    pull(controller) {
      const chunk = pending.shift();
      if (chunk !== undefined) controller.enqueue(sseEncoder.encode(chunk));
      else controller.close();
    },
  });
  const calls: Array<{ status: number; meta: unknown }> = [];
  const body = await new Response(tapAnthropicSseForLog(
    upstream,
    freshLogCtx(),
    (status, meta) => calls.push({ status, meta }),
    { stallMs: 5_000, maxBytes: 0 },
  )).text();

  expect(body).toBe(expectedBody);
  expect(calls).toEqual([{
    status: expectedStatus,
    meta: { closeReason: "terminal", terminalStatus: expectedTerminal },
  }]);
});

test("A0d: one large upstream read relays at most one validated frame per pull", async () => {
  const frame = `data: {"type":"content_block_delta","padding":"${"x".repeat(2_048)}"}\n\n`;
  const chunk = sseEncoder.encode(frame.repeat(2_049) + MESSAGE_STOP_FRAME);
  expect(chunk.byteLength).toBeGreaterThan(4 * 1024 * 1024);
  const upstream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(chunk);
      controller.close();
    },
  });
  const { calls, finalize } = spyFinalize();
  const reader = tapAnthropicSseForLog(
    upstream,
    freshLogCtx(),
    finalize,
    { stallMs: 5_000, maxBytes: 0 },
  ).getReader();

  try {
    const first = await reader.read();
    expect(first.done).toBe(false);
    expect(new TextDecoder().decode(first.value)).toBe(frame);
    expect(calls).toEqual([]);
  } finally {
    await reader.cancel();
  }
});

test("A0e: Anthropic reader rejection appends one failed error terminal", async () => {
  let pull = 0;
  const upstream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (pull++ === 0) controller.enqueue(sseEncoder.encode(MESSAGE_START_FRAME));
      else controller.error(new Error("wire broke"));
    },
  });
  const calls: Array<{ status: number; meta: unknown }> = [];
  const body = await new Response(tapAnthropicSseForLog(
    upstream,
    freshLogCtx(),
    (status, meta) => calls.push({ status, meta }),
    { stallMs: 5_000, maxBytes: 0 },
  )).text();

  expect(body.startsWith(MESSAGE_START_FRAME)).toBe(true);
  expect(body).toContain("anthropic passthrough body failed: wire broke");
  expect((body.match(/event: error/g) ?? [])).toHaveLength(1);
  expect(calls).toEqual([{
    status: 502,
    meta: { closeReason: "terminal", terminalStatus: "failed" },
  }]);
});

test("A0f: malformed Anthropic SSE records fail before relay or later terminal", async () => {
  const invalidUtf8 = Uint8Array.from([
    ...sseEncoder.encode("data: \"bad_utf8_"),
    0xc3,
    0x28,
    ...sseEncoder.encode("\"\n\n"),
  ]);
  const cases = [
    { name: "invalid UTF-8", frame: invalidUtf8, marker: "bad_utf8_" },
    { name: "malformed JSON", frame: sseEncoder.encode("data: {bad_json_marker\n\n"), marker: "bad_json_marker" },
    { name: "scalar JSON", frame: sseEncoder.encode('data: "bad_scalar_marker"\n\n'), marker: "bad_scalar_marker" },
    { name: "array JSON", frame: sseEncoder.encode('data: ["bad_array_marker"]\n\n'), marker: "bad_array_marker" },
    { name: "Responses sentinel", frame: sseEncoder.encode("data: [DONE]\n\n"), marker: "[DONE]" },
  ];

  for (const testCase of cases) {
    let pulled = false;
    let cancels = 0;
    const chunk = Uint8Array.from([
      ...sseEncoder.encode(MESSAGE_START_FRAME),
      ...testCase.frame,
      ...sseEncoder.encode(MESSAGE_STOP_FRAME),
    ]);
    const upstream = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (pulled) return;
        pulled = true;
        controller.enqueue(chunk);
      },
      cancel() { cancels += 1; },
    }, { highWaterMark: 0 });
    const calls: Array<{ status: number; meta: unknown }> = [];
    const body = await new Response(tapAnthropicSseForLog(
      upstream,
      freshLogCtx(),
      (status, meta) => calls.push({ status, meta }),
      { stallMs: 5_000, maxBytes: 0 },
    )).text();

    expect(body.startsWith(MESSAGE_START_FRAME), testCase.name).toBe(true);
    expect(body, testCase.name).not.toContain(testCase.marker);
    expect(body, testCase.name).not.toContain("message_stop");
    expect(body.match(/event: error/g) ?? [], testCase.name).toHaveLength(1);
    expect(cancels, testCase.name).toBe(1);
    expect(calls, testCase.name).toEqual([{
      status: 502,
      meta: { closeReason: "terminal", terminalStatus: "failed" },
    }]);
  }
});

test("A1: stalled upstream body gets an Anthropic timeout_error tail and body_stall close reason", async () => {
  const upstream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(sseEncoder.encode(MESSAGE_START_FRAME));
      // never closes, never enqueues again — a dead-but-open upstream
    },
  });
  const { calls, finalize } = spyFinalize();
  const tap = tapAnthropicSseForLog(upstream, freshLogCtx(), finalize, { stallMs: 30, maxBytes: 0 });
  const text = await new Response(tap).text();
  expect(text).toContain("message_start"); // prior bytes preserved
  expect(text).toContain("\n\nevent: error\ndata: ");
  expect(text).toContain('"type":"timeout_error"');
  expect(calls).toEqual([{ status: 504, closeReason: "body_stall" }]);
});

test("A2: unbounded upstream body gets an api_error tail and body_overflow close reason", async () => {
  const flood = new ReadableStream<Uint8Array>({
    pull(controller) {
      controller.enqueue(sseEncoder.encode('data: {"type":"content_block_delta"}\n\n'));
    },
  });
  const { calls, finalize } = spyFinalize();
  const tap = tapAnthropicSseForLog(flood, freshLogCtx(), finalize, { stallMs: 0, maxBytes: 120 });
  const text = await new Response(tap).text();
  expect(text).toContain("\n\nevent: error\ndata: ");
  expect(text).toContain('"type":"api_error"');
  expect(text).toContain("exceeded 120 bytes");
  expect(calls).toEqual([{ status: 502, closeReason: "body_overflow" }]);
});

test("A2b: an unterminated over-cap SSE frame fails as overflow before idle stall", async () => {
  const partial = sseEncoder.encode(`data: {"type":"content_block_delta","padding":"${"x".repeat(120)}`);
  expect(partial.byteLength).toBeGreaterThan(120);
  const upstream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(partial);
    },
  });
  const { calls, finalize } = spyFinalize();
  const text = await new Response(tapAnthropicSseForLog(
    upstream,
    freshLogCtx(),
    finalize,
    { stallMs: 30, maxBytes: 120 },
  )).text();

  expect(text).toContain("anthropic passthrough body exceeded 120 bytes");
  expect(text).not.toContain("body stalled");
  expect(calls).toEqual([{ status: 502, closeReason: "body_overflow" }]);
});

test("A3: client abort mid-body finalizes 499 client_cancel, not 200 terminal (misclassification regression)", async () => {
  let upstreamCancelled = false;
  const upstream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(sseEncoder.encode(MESSAGE_START_FRAME));
    },
    cancel() {
      upstreamCancelled = true;
    },
  });
  const ac = new AbortController();
  const { calls, finalize } = spyFinalize();
  const tap = tapAnthropicSseForLog(upstream, freshLogCtx(), finalize, { stallMs: 5_000, maxBytes: 0, reqSignal: ac.signal });
  const reader = tap.getReader();
  const first = await reader.read();
  expect(first.done).toBe(false);
  ac.abort(new DOMException("client went away", "AbortError"));
  // drain to settlement: onClientAbort closes the tap
  while (!(await reader.read()).done) { /* drain */ }
  expect(calls).toEqual([{ status: 499, closeReason: "client_cancel" }]);
  expect(upstreamCancelled).toBe(true);
});

test("A4: slow-but-alive stream outlives many idle windows (anti-total-wall-clock invariant)", async () => {
  let sent = 0;
  const upstream = new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (sent === 0) {
        controller.enqueue(sseEncoder.encode(MESSAGE_START_FRAME));
        sent += 1;
        return;
      }
      if (sent < 7) {
        await new Promise(resolve => setTimeout(resolve, 50)); // silence (50ms) << stallMs (200ms), total (300ms) >> stallMs
        controller.enqueue(sseEncoder.encode('data: {"type":"content_block_delta"}\n\n'));
        sent += 1;
        return;
      }
      controller.enqueue(sseEncoder.encode('event: message_stop\ndata: {"type":"message_stop"}\n\n'));
      controller.close();
    },
  });
  const { calls, finalize } = spyFinalize();
  const tap = tapAnthropicSseForLog(upstream, freshLogCtx(), finalize, { stallMs: 200, maxBytes: 0 });
  const text = await new Response(tap).text();
  expect(text).toContain("message_stop");
  expect(text).not.toContain("event: error");
  expect(calls).toEqual([{ status: 200, closeReason: "terminal" }]);
});

test("A5: non-stream bounded read classifies stall and overflow, passes clean bodies through", async () => {
  const stalling = new Response(new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(sseEncoder.encode('{"partial":'));
    },
  }));
  expect(await readBoundedPassthroughBody(stalling, { stallMs: 30, maxBytes: 0 })).toEqual({ kind: "stall" });

  const flooding = new Response(new ReadableStream<Uint8Array>({
    pull(controller) {
      controller.enqueue(sseEncoder.encode("x".repeat(40)));
    },
  }));
  expect(await readBoundedPassthroughBody(flooding, { stallMs: 0, maxBytes: 100 })).toEqual({ kind: "overflow" });

  const clean = new Response('{"usage":{"input_tokens":1}}');
  expect(await readBoundedPassthroughBody(clean, { stallMs: 1_000, maxBytes: 1_000 }))
    .toEqual({ kind: "ok", text: '{"usage":{"input_tokens":1}}' });

  // Client abort mid-read classifies deterministically (audit round 4 blocker) —
  // including the pre-aborted-signal path.
  const ac = new AbortController();
  const hanging = new Response(new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(sseEncoder.encode('{"partial":'));
    },
  }));
  const pending = readBoundedPassthroughBody(hanging, { stallMs: 5_000, maxBytes: 0, reqSignal: ac.signal });
  setTimeout(() => ac.abort(new DOMException("client went away", "AbortError")), 20);
  expect(await pending).toEqual({ kind: "client_cancel" });

  const preAborted = new AbortController();
  preAborted.abort();
  const neverRead = new Response(new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(sseEncoder.encode("x"));
    },
  }));
  expect(await readBoundedPassthroughBody(neverRead, { stallMs: 5_000, maxBytes: 0, reqSignal: preAborted.signal }))
    .toEqual({ kind: "client_cancel" });
});

test("A5b: non-stream reads retain geometrically instead of one object per transport chunk", async () => {
  const totalBytes = 70 * 1024;
  const readWithChunkSize = async (chunkSize: number) => {
    let emitted = 0;
    const result = await readBoundedPassthroughBytes(new Response(new ReadableStream<Uint8Array>({
      pull(controller) {
        if (emitted >= totalBytes) {
          controller.close();
          return;
        }
        const size = Math.min(chunkSize, totalBytes - emitted);
        const chunk = new Uint8Array(size);
        chunk.fill((emitted / chunkSize) % 251);
        emitted += size;
        controller.enqueue(chunk);
      },
    }, { highWaterMark: 0 })), { stallMs: 0, maxBytes: totalBytes });
    expect(result.kind).toBe("ok");
    if (result.kind === "ok") expect(result.bytes.byteLength).toBe(totalBytes);
    return passthroughBodyBufferGrowthsForTests();
  };

  const fineGrowths = await readWithChunkSize(1);
  const coarseGrowths = await readWithChunkSize(4 * 1024);
  expect(fineGrowths).toBe(1);
  expect(coarseGrowths).toBe(fineGrowths);
});

test("A6: body-guard config normalization — 0 disables, negatives fall back, sub-second clamps to 1s", () => {
  const guardFor = (claudeCode: OcxConfig["claudeCode"]) =>
    resolvePassthroughBodyGuard(mockConfig("http://127.0.0.1:1/v1", claudeCode));
  expect(guardFor({ bodyStallSec: 0, bodyMaxBytes: 0 })).toMatchObject({ stallMs: 0, maxBytes: 0 });
  expect(guardFor({ bodyStallSec: -5, bodyMaxBytes: -1 })).toMatchObject({ stallMs: 90_000, maxBytes: 64 * 1024 * 1024 });
  expect(guardFor({ bodyStallSec: 0.5, bodyMaxBytes: 1024.9 })).toMatchObject({ stallMs: 1_000, maxBytes: 1024 });
  expect(guardFor(undefined)).toMatchObject({ stallMs: 90_000, maxBytes: 64 * 1024 * 1024 });
  expect(guardFor({ bodyStallSec: Number.NaN, bodyMaxBytes: Number.POSITIVE_INFINITY }))
    .toMatchObject({ stallMs: 90_000, maxBytes: 64 * 1024 * 1024 });
});

test("synthetic error tail parses as a terminal error in the Anthropic dialect (adapter fixture proof)", async () => {
  const adapter = createAnthropicAdapter({ adapter: "anthropic", baseUrl: "https://example.test", apiKey: "key" });
  const response = new Response([
    MESSAGE_START_FRAME,
    '\n\nevent: error\ndata: {"type":"error","error":{"type":"timeout_error","message":"anthropic passthrough body stalled: no upstream bytes for 90s"}}\n\n',
  ].join(""));
  const events: Array<{ type: string }> = [];
  for await (const event of adapter.parseStream(response, createTestTranslatorBudget())) events.push(event);
  const errorIndex = events.findIndex(e => e.type === "error");
  expect(errorIndex).toBeGreaterThanOrEqual(0);
  expect(events.slice(errorIndex + 1).filter(e => e.type === "done")).toHaveLength(0);
});

test("endpoint wiring: configured bodyStallSec bounds a stalled native passthrough stream", async () => {
  const upstream = Bun.serve({
    port: 0,
    fetch() {
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(sseEncoder.encode(MESSAGE_START_FRAME));
          // stalls forever
        },
      });
      return new Response(body, { headers: { "content-type": "text/event-stream" } });
    },
  });
  const config = mockConfig("http://127.0.0.1:1/v1", {
    anthropicBaseUrl: upstream.url.toString().replace(/\/$/, ""),
    bodyStallSec: 1,
  });
  saveConfig(config);
  const server = startServer(0);
  try {
    const response = await fetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "sk-ant-test" },
      body: JSON.stringify({
        model: "claude-test",
        max_tokens: 16,
        stream: true,
        messages: [{ role: "user", content: "hi" }],
      }),
    });
    expect(response.status).toBe(200);
    const text = await response.text();
    expect(text).toContain("event: error");
    expect(text).toContain("timeout_error");
  } finally {
    await server.stop(true);
    upstream.stop(true);
  }
});

test("native openai-responses route carries prompt_cache_key + synthesized session_id header", async () => {
  const capture: { headers?: Record<string, string>; body?: Record<string, any> } = {};
  const upstream = Bun.serve({
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);
      if (!url.pathname.endsWith("/responses")) {
        return Response.json({ error: { message: `unexpected path ${url.pathname}` } }, { status: 404 });
      }
      capture.headers = Object.fromEntries(req.headers);
      capture.body = await req.json() as Record<string, any>;
      const frames = [
        `event: response.created\ndata: ${JSON.stringify({ response: { id: "resp_1", status: "in_progress" } })}\n\n`,
        `event: response.output_text.delta\ndata: ${JSON.stringify({ delta: "Hello" })}\n\n`,
        `event: response.completed\ndata: ${JSON.stringify({ response: { status: "completed", usage: { input_tokens: 10, output_tokens: 2 } } })}\n\n`,
      ];
      return new Response(frames.join(""), { headers: { "Content-Type": "text/event-stream" } });
    },
  });
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const requestUrl = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    const url = new URL(requestUrl);
    if (url.origin === "https://chatgpt.com") {
      if (url.pathname !== "/backend-api/codex/responses") {
        throw new Error(`unexpected canonical Codex path ${url.pathname}`);
      }
      return originalFetch(new URL("/responses", upstream.url), init);
    }
    return originalFetch(input, init);
  }) as typeof fetch;
  saveConfig({
    port: 0,
    defaultProvider: "native",
    providers: {
      native: { adapter: "openai-responses", baseUrl: "https://chatgpt.com/backend-api/codex", authMode: "forward" },
    },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await fetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "native/gpt-test",
        max_tokens: 128,
        temperature: 0.7,
        top_p: 0.9,
        stop_sequences: ["DONE"],
        messages: [{ role: "user", content: "hi" }],
        metadata: { user_id: "user_abc123_account__session_11111111-2222-3333-4444-555555555555" },
        thinking: { type: "adaptive", display: "omitted" },
        output_config: { effort: "high" },
      }),
    });
    expect(response.status).toBe(200);
    await response.text();
    // Native ChatGPT route: sampling params + user are stripped, but the cache-affinity
    // pair survives — prompt_cache_key in the body and a synthesized session_id header
    // (devlog 090: without the header the backend reported cached_tokens: 0 every turn).
    expect(capture.body?.prompt_cache_key).toMatch(/^[0-9a-f]{32}$/);
    expect(capture.body?.user).toBeUndefined();
    expect(capture.body?.max_output_tokens).toBeUndefined();
    expect(capture.body?.temperature).toBeUndefined();
    expect(capture.body?.top_p).toBeUndefined();
    expect(capture.body?.stop).toBeUndefined();
    expect(capture.body?.reasoning?.effort).toBe("high");
    expect(capture.headers?.["session_id"]).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-8[0-9a-f]{3}-[0-9a-f]{12}$/);
  } finally {
    globalThis.fetch = originalFetch;
    await server.stop(true);
    upstream.stop(true);
  }
});

test("combo openai-responses target strips user but preserves prompt_cache_key", async () => {
  const capture: { body?: Record<string, unknown> } = {};
  let returnTopLevelError = false;
  const claudeBody = {
    model: "claude-haiku-4-5",
    max_tokens: 128,
    temperature: 0.7,
    top_p: 0.9,
    stop_sequences: ["DONE"],
    messages: [{ role: "user", content: "hi" }],
    metadata: { user_id: "user_abc123_account__session_11111111-2222-3333-4444-555555555555" },
  };
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(input, init);
    const url = new URL(request.url);
    if (url.origin === "https://chatgpt.com") {
      expect(url.pathname).toBe("/backend-api/codex/responses");
      capture.body = await request.json() as Record<string, unknown>;
      if (returnTopLevelError) {
        return new Response(
          `event: error\ndata: ${JSON.stringify({
            type: "error",
            error: { type: "invalid_request_error", message: "Unsupported parameter: user" },
          })}\n\n`,
          { headers: { "content-type": "text/event-stream" } },
        );
      }
      return new Response([
        `event: response.created\ndata: ${JSON.stringify({ response: { id: "resp_1", status: "in_progress" } })}\n\n`,
        `event: response.output_text.delta\ndata: ${JSON.stringify({ delta: "Hello" })}\n\n`,
        `event: response.completed\ndata: ${JSON.stringify({ response: { status: "completed", usage: { input_tokens: 10, output_tokens: 2 } } })}\n\n`,
      ].join(""), { headers: { "content-type": "text/event-stream" } });
    }
    return originalFetch(input, init);
  }) as typeof fetch;
  saveConfig({
    port: 0,
    defaultProvider: "native",
    providers: {
      native: {
        adapter: "openai-responses",
        baseUrl: "https://chatgpt.com/backend-api/codex",
        authMode: "forward",
      },
    },
    combos: {
      waterfall: {
        strategy: "failover",
        targets: [{ provider: "native", model: "gpt-test" }],
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "combo/waterfall" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(claudeBody),
    });
    expect(response.status).toBe(200);
    await response.text();
    expect(capture.body?.user).toBeUndefined();
    expect(capture.body?.max_output_tokens).toBeUndefined();
    expect(capture.body?.temperature).toBeUndefined();
    expect(capture.body?.top_p).toBeUndefined();
    expect(capture.body?.stop).toBeUndefined();
    expect(capture.body?.prompt_cache_key).toMatch(/^[0-9a-f]{32}$/);
    expect(getRequestLogEntries().findLast(row => row.surface === "claude")?.attempts).toMatchObject([
      { provider: "native", model: "gpt-test", sendCount: 1 },
    ]);

    returnTopLevelError = true;
    const failure = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(claudeBody),
    });
    expect(failure.status).toBe(400);
    const failureBody = await failure.text();
    expect(failureBody).toContain("Unsupported parameter: user");
    expect(failureBody).not.toContain("adapter_eof");
  } finally {
    await server.stop(true);
  }
});

test("native openai-responses Claude route logs cyber terminals as 400 cyber_policy", async () => {
  clearRequestLogsForTests();
  const upstream = Bun.serve({
    port: 0,
    fetch() {
      return new Response([
        "event: response.failed",
        `data: ${JSON.stringify({
          type: "response.failed",
          response: {
            status: "failed",
            error: { type: "invalid_request_error", code: "cyber_policy", message: "blocked" },
          },
        })}`,
        "",
        "",
      ].join("\n"), { headers: { "Content-Type": "text/event-stream" } });
    },
  });
  saveConfig({
    port: 0,
    defaultProvider: "native",
    providers: {
      native: {
        adapter: "openai-responses",
        baseUrl: `${upstream.url.toString().replace(/\/$/, "")}/v1`,
        authMode: "forward",
        allowPrivateNetwork: true,
      },
    },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await fetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "native/gpt-test",
        max_tokens: 128,
        stream: true,
        messages: [{ role: "user", content: "hi" }],
      }),
    });
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("blocked");
    const entry = getRequestLogEntries().findLast(e => e.surface === "claude");
    expect(entry).toMatchObject({
      status: 400,
      errorCode: "cyber_policy",
      terminalStatus: "failed",
      closeReason: "terminal",
      upstreamError: "blocked",
    });
  } finally {
    await server.stop(true);
    await upstream.stop(true);
    clearRequestLogsForTests();
  }
});

test("custom forward openai-responses route never receives the main ChatGPT credential", async () => {
  writeFileSync(join(isolatedCodexHome!.path, "auth.json"), JSON.stringify({
    tokens: { access_token: "main-secret-must-not-leave", account_id: "main-account-must-not-leave" },
  }));
  const captured: Array<{
    authorization: string | null;
    accountId: string | null;
    body: Record<string, unknown>;
  }> = [];
  const upstream = Bun.serve({
    port: 0,
    async fetch(req) {
      captured.push({
        authorization: req.headers.get("authorization"),
        accountId: req.headers.get("chatgpt-account-id"),
        body: await req.json() as Record<string, unknown>,
      });
      return new Response([
        'event: response.created\ndata: {"response":{"id":"resp_1","status":"in_progress"}}\n\n',
        'event: response.output_text.delta\ndata: {"delta":"ok"}\n\n',
        'event: response.completed\ndata: {"response":{"status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}\n\n',
      ].join(""), { headers: { "content-type": "text/event-stream" } });
    },
  });
  saveConfig({
    port: 0,
    defaultProvider: "custom",
    providers: {
      custom: {
        adapter: "openai-chat",
        baseUrl: `${upstream.url.toString().replace(/\/$/, "")}/v1`,
        authMode: "forward",
        allowPrivateNetwork: true,
        modelAdapters: { "gpt-test": "openai-responses" },
      },
    },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await fetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", authorization: "Bearer claude-placeholder" },
      body: JSON.stringify({
        model: "custom/gpt-test",
        max_tokens: 16,
        temperature: 0.7,
        top_p: 0.9,
        stop_sequences: ["DONE"],
        metadata: { user_id: "custom-user" },
        messages: [{ role: "user", content: "hi" }],
      }),
    });
    const responseBody = await response.text();
    expect({ status: response.status, body: responseBody }).toMatchObject({ status: 200 });
    expect(captured).toHaveLength(1);
    expect(captured[0]).toMatchObject({ authorization: null, accountId: null });
    expect(captured[0]!.body).toMatchObject({
      max_output_tokens: 16,
      temperature: 0.7,
      top_p: 0.9,
      stop: ["DONE"],
      user: "custom-user",
    });
    expect(captured[0]!.body).not.toHaveProperty("metadata");
  } finally {
    await server.stop(true);
    upstream.stop(true);
  }
});

test("shadow-call rerouting cannot carry the main ChatGPT credential to a custom forward route", async () => {
  writeFileSync(join(isolatedCodexHome!.path, "auth.json"), JSON.stringify({
    tokens: { access_token: "main-secret-must-not-leave", account_id: "main-account-must-not-leave" },
  }));
  const captured: Array<{ authorization: string | null; accountId: string | null }> = [];
  const upstream = Bun.serve({
    port: 0,
    fetch(req) {
      captured.push({
        authorization: req.headers.get("authorization"),
        accountId: req.headers.get("chatgpt-account-id"),
      });
      return new Response([
        'event: response.created\ndata: {"response":{"id":"resp_1","status":"in_progress"}}\n\n',
        'event: response.output_text.delta\ndata: {"delta":"ok"}\n\n',
        'event: response.completed\ndata: {"response":{"status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}\n\n',
      ].join(""), { headers: { "content-type": "text/event-stream" } });
    },
  });
  saveConfig({
    port: 0,
    defaultProvider: "openai",
    providers: {
      openai: {
        adapter: "openai-responses",
        baseUrl: "https://chatgpt.com/backend-api/codex",
        authMode: "forward",
        codexAccountMode: "direct",
      },
      custom: {
        adapter: "openai-chat",
        baseUrl: `${upstream.url.toString().replace(/\/$/, "")}/v1`,
        authMode: "forward",
        allowPrivateNetwork: true,
        modelAdapters: { "gpt-test": "openai-responses" },
      },
    },
    shadowCallIntercept: {
      enabled: true,
      model: "custom/gpt-test",
      sourceModels: ["gpt-5.6-luna"],
    },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await fetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", authorization: "Bearer claude-placeholder" },
      body: JSON.stringify({
        model: "gpt-5.6-luna",
        max_tokens: 16,
        messages: [{ role: "user", content: "hi" }],
      }),
    });
    const responseBody = await response.text();
    expect({ status: response.status, body: responseBody }).toMatchObject({ status: 200 });
    expect(captured).toEqual([{ authorization: null, accountId: null }]);
  } finally {
    await server.stop(true);
    upstream.stop(true);
  }
});

/**
 * This case sandboxes CODEX_HOME, so the service installed on the developer's
 * machine is not evidence about it. See tests/helpers/owned-service-home.ts.
 */
const inspectNativeCodexOwnership = ownedServiceHomeInspection("claude replay main-enrichment test");

test("Claude replay owns optional main enrichment while routed work survives drain and recovery", async () => {
  resetLifecycleDrainStateForTests();
  writeFileSync(join(isolatedCodexHome!.path, "auth.json"), JSON.stringify({
    tokens: { access_token: "claude-main-access", account_id: "claude-main-account" },
  }));
  let upstreamCalls = 0;
  let finishUpstream: (() => void) | undefined;
  let markStarted!: () => void;
  const started = new Promise<void>(resolve => { markStarted = resolve; });
  const encoder = new TextEncoder();
  const upstream = Bun.serve({
    port: 0,
    fetch() {
      upstreamCalls += 1;
      if (upstreamCalls > 1) {
        return new Response('data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n', {
          headers: { "content-type": "text/event-stream" },
        });
      }
      return new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(encoder.encode('data: {"choices":[{"index":0,"delta":{"content":"held"}}]}\n\n'));
          finishUpstream = () => {
            finishUpstream = undefined;
            controller.enqueue(encoder.encode('data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n'));
            controller.close();
          };
          markStarted();
        },
      }), { headers: { "content-type": "text/event-stream" } });
    },
  });
  saveConfig(mockConfig(`${upstream.url.toString().replace(/\/$/, "")}/v1`));
  let server = startServer(0, { inspectNativeCodexOwnership });
  await waitForNativeMainStartupGate();
  let drain: ReturnType<typeof acquireNativeMainProfileDrain> = null;
  let recoveryHomeId: string | null = null;
  try {
    await waitForNativeMainStartupGate();
    const pending = postMessages(server.url.toString(), {
      model: "mock/test-model",
      max_tokens: 64,
      stream: true,
      messages: [{ role: "user", content: "hold" }],
    });
    await started;
    const response = await pending;
    expect(response.status).toBe(200);
    expect(getNativeMainProfileRequestCount()).toBe(1);
    drain = acquireNativeMainProfileDrain("claude-overlap");
    expect(drain).not.toBeNull();
    const routedDuringDrain = await postMessages(server.url.toString(), {
      model: "mock/test-model",
      max_tokens: 64,
      stream: true,
      messages: [{ role: "user", content: "routed during drain" }],
    });
    expect(routedDuringDrain.status).toBe(200);
    await routedDuringDrain.text();
    expect(upstreamCalls).toBe(2);

    finishUpstream?.();
    await response.text();
    expect(getNativeMainProfileRequestCount()).toBe(0);
    drain?.release();
    drain = null;

    recoveryHomeId = nativeMainStartupGateSnapshot().homeId ?? "claude-recovery-home";
    expect(blockNativeMainRecovery(recoveryHomeId, "manual")).toBe(true);
    const routedDuringRecovery = await postMessages(server.url.toString(), {
      model: "mock/test-model",
      max_tokens: 64,
      stream: false,
      messages: [{ role: "user", content: "routed during recovery" }],
    });
    expect(routedDuringRecovery.status).toBe(200);
    expect(upstreamCalls).toBe(3);

    completeNativeMainRecovery(recoveryHomeId);
    recoveryHomeId = null;
    await server.stop(true);
    saveConfig({
      port: 0,
      openaiProviderTierVersion: 2,
      defaultProvider: "openai",
      providers: {
        openai: {
          adapter: "openai-responses",
          baseUrl: "https://chatgpt.com/backend-api/codex",
          authMode: "forward",
          codexAccountMode: "pool",
        },
      },
      codexAccounts: [],
      activeCodexAccountId: "__main__",
      autoSwitchThreshold: 0,
    } as OcxConfig);
    server = startServer(0, { inspectNativeCodexOwnership });
    await waitForNativeMainStartupGate();
    recoveryHomeId = nativeMainStartupGateSnapshot().homeId ?? "claude-main-recovery-home";
    expect(blockNativeMainRecovery(recoveryHomeId, "manual")).toBe(true);
    const mainBlocked = await postMessages(server.url.toString(), {
      model: "openai/gpt-test",
      max_tokens: 64,
      stream: false,
      messages: [{ role: "user", content: "main blocked" }],
    });
    expect(mainBlocked.status).toBe(503);
    expect(upstreamCalls).toBe(3);
  } finally {
    if (recoveryHomeId) completeNativeMainRecovery(recoveryHomeId);
    drain?.release();
    finishUpstream?.();
    await server.stop(true);
    upstream.stop(true);
    resetLifecycleDrainStateForTests();
  }
});

test("routed Claude requests give OpenAI sidecars main auth without leaking it to the routed provider", async () => {
  const mainAccessToken = "main-chatgpt-access";
  const mainAccountId = "main-chatgpt-account";
  const imageBytes = "aGVsbG8taW1hZ2UtYnl0ZXM=";
  const visionCaption = "A red OPENCODEX logo on a white background.";
  const sidecarCalls: Array<{ headers: Headers; body: Record<string, any>; kind: "vision" | "web-search" }> = [];
  const routedCalls: Array<{ authorization: string | null; body: Record<string, any> }> = [];

  const forward = Bun.serve({
    port: 0,
    async fetch(req) {
      const body = await req.json() as Record<string, any>;
      const kind = Array.isArray(body.tools) && body.tools.some((tool: Record<string, unknown>) => tool.type === "web_search")
        ? "web-search"
        : "vision";
      sidecarCalls.push({ headers: new Headers(req.headers), body, kind });
      const text = kind === "vision" ? visionCaption : "OpenCodex search results are available.";
      return new Response([
        `event: response.output_text.delta\ndata: ${JSON.stringify({ type: "response.output_text.delta", delta: text })}\n\n`,
        `event: response.completed\ndata: ${JSON.stringify({ type: "response.completed", response: { status: "completed" } })}\n\n`,
      ].join(""), { headers: { "content-type": "text/event-stream" } });
    },
  });
  const routed = Bun.serve({
    port: 0,
    async fetch(req) {
      const body = await req.json() as Record<string, any>;
      routedCalls.push({ authorization: req.headers.get("authorization"), body });
      const choosesWebSearch = routedCalls.length === 1
        && Array.isArray(body.tools)
        && body.tools.some((tool: Record<string, any>) => tool.function?.name === "web_search");
      const frames = choosesWebSearch
        ? [
            { choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: "call_search", function: { name: "web_search", arguments: '{"query":"latest opencodex"}' } }] } }] },
            { choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] },
          ]
        : [
            { choices: [{ index: 0, delta: { content: "Routed answer" } }] },
            { choices: [{ index: 0, delta: {}, finish_reason: "stop" }] },
          ];
      return new Response(
        frames.map(frame => `data: ${JSON.stringify(frame)}\n\n`).join("") + "data: [DONE]\n\n",
        { headers: { "content-type": "text/event-stream" } },
      );
    },
  });

  const config = {
    port: 0,
    defaultProvider: "routed",
    openaiProviderTierVersion: 2,
    providers: {
      openai: {
        adapter: "openai-responses",
        authMode: "forward",
        baseUrl: "https://chatgpt.com/backend-api/codex",
        codexAccountMode: "pool",
      },
      routed: {
        adapter: "openai-chat",
        baseUrl: `${routed.url.toString().replace(/\/$/, "")}/v1`,
        apiKey: "routed-provider-key",
        allowPrivateNetwork: true,
        noVisionModels: ["text-model"],
      },
    },
    webSearchSidecar: { backend: "openai" },
    visionSidecar: { backend: "openai" },
  } as OcxConfig;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const requestUrl = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    const url = new URL(requestUrl);
    const prefix = "/backend-api/codex";
    if (url.hostname === "chatgpt.com" && url.pathname.startsWith(prefix)) {
      return originalFetch(new URL(`${url.pathname.slice(prefix.length)}${url.search}`, forward.url), init);
    }
    return originalFetch(input, init);
  }) as typeof fetch;
  saveConfig(config);
  writeFileSync(join(isolatedCodexHome!.path, "auth.json"), JSON.stringify({
    tokens: { access_token: mainAccessToken, account_id: mainAccountId },
  }));
  let requestSequence = 0;
  const requestBody = {
    model: "routed/text-model",
    max_tokens: 128,
    stream: false,
    tools: [{ type: "web_search_20250305", name: "web_search" }],
    messages: [{
      role: "user",
      content: [
        { type: "text", text: "Search for OpenCodex and inspect this logo." },
        { type: "image", source: { type: "base64", media_type: "image/png", data: imageBytes } },
      ],
    }],
  };
  const invokeMessages = async (): Promise<number> => {
    const turnAdmissionLease = tryAdmitTurn();
    if (!turnAdmissionLease) throw new Error("test turn admission unavailable");
    const start = Date.now();
    try {
      const response = await handleClaudeMessages(
        new Request("http://localhost/v1/messages", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-api-key": "placeholder",
            "anthropic-version": "2023-06-01",
          },
          body: JSON.stringify(requestBody),
        }),
        config,
        { model: "unknown", provider: "unknown", inboundProtocol: "messages" } as RequestLogContext,
        { requestId: `claude-sidecar-test-${++requestSequence}`, start, turnAdmissionLease },
      );
      await response.text();
      return response.status;
    } finally {
      turnAdmissionLease.release();
    }
  };
  try {
    expect(await invokeMessages()).toBe(200);

    expect(sidecarCalls.map(call => call.kind).sort()).toEqual(["vision", "web-search"]);
    for (const call of sidecarCalls) {
      expect(call.headers.get("authorization")).toBe(`Bearer ${mainAccessToken}`);
      expect(call.headers.get("chatgpt-account-id")).toBe(mainAccountId);
    }
    expect(sidecarCalls.find(call => call.kind === "vision")?.body.input).toEqual(expect.any(Array));
    expect(sidecarCalls.find(call => call.kind === "web-search")?.body.tools?.[0]?.type).toBe("web_search");
    expect(routedCalls.length).toBe(2);
    expect(routedCalls.every(call => call.authorization === "Bearer routed-provider-key")).toBe(true);
    const authenticatedRoutedBodies = JSON.stringify(routedCalls.map(call => call.body));
    expect(authenticatedRoutedBodies).toContain(visionCaption);
    expect(authenticatedRoutedBodies).not.toContain("[image omitted:");
    expect(authenticatedRoutedBodies).not.toContain(imageBytes);

    rmSync(join(isolatedCodexHome!.path, "auth.json"));
    const sidecarCountBeforeNoLogin = sidecarCalls.length;
    expect(await invokeMessages()).toBe(200);

    expect(sidecarCalls.length).toBe(sidecarCountBeforeNoLogin);
    expect(routedCalls.at(-1)?.authorization).toBe("Bearer routed-provider-key");
    const noLoginBody = JSON.stringify(routedCalls.at(-1)?.body);
    expect(noLoginBody).toContain("[image omitted: this model is text-only and the vision sidecar is unavailable (no ChatGPT login)]");
    expect(noLoginBody).not.toContain(imageBytes);
  } finally {
    await forward.stop(true);
    await routed.stop(true);
  }
});

test("bad body -> Anthropic-shaped 400; unknown /v1 path guard intact", async () => {
  saveConfig(mockConfig("http://127.0.0.1:1/v1"));
  const server = startServer(0);
  try {
    const bad = await fetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ max_tokens: 5, messages: [{ role: "user", content: "x" }] }),
    });
    expect(bad.status).toBe(400);
    const badJson = await bad.json() as Record<string, any>;
    expect(badJson).toEqual({ type: "error", error: { type: "invalid_request_error", message: "model is required" } });

    const unknown = await fetch(new URL("/v1/does-not-exist", server.url), { method: "POST" });
    expect(unknown.status).toBe(404);
  } finally {
    await server.stop(true);
  }
});

test("count_tokens returns a positive estimate in the exact contract shape", async () => {
  saveConfig(mockConfig("http://127.0.0.1:1/v1"));
  const server = startServer(0);
  try {
    const response = await fetch(new URL("/v1/messages/count_tokens", server.url), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "mock/test-model",
        system: "be brief",
        messages: [{ role: "user", content: "count me please, this is a sentence" }],
        tools: [{ name: "Read", input_schema: { type: "object" } }],
      }),
    });
    expect(response.status).toBe(200);
    const json = await response.json() as Record<string, unknown>;
    expect(Object.keys(json)).toEqual(["input_tokens"]);
    expect(json.input_tokens as number).toBeGreaterThan(0);
  } finally {
    await server.stop(true);
  }
});

test("mapped combo count_tokens uses exact canonical Anthropic counting", async () => {
  const captured: Array<{
    url: string;
    headers: Headers;
    body: Record<string, unknown>;
    keepalive?: boolean;
  }> = [];
  const upstreamBody = JSON.stringify({
    type: "error",
    error: { type: "rate_limit_error", message: "count limit" },
  });
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages/count_tokens") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    captured.push({
      url: request.url,
      headers: request.headers,
      body: await request.json() as Record<string, unknown>,
      keepalive: init?.keepalive,
    });
    return new Response(upstreamBody, {
      status: 429,
      headers: { "content-type": "application/json", "retry-after": "17" },
    });
  };
  writeFileSync(join(testDir, "auth.json"), JSON.stringify({
    anthropic: {
      activeAccountId: "count-account",
      accounts: [{
        id: "count-account",
        credential: {
          access: "selected-count-token",
          refresh: "test-refresh-token",
          expires: 9999999999999,
        },
      }],
    },
  }), { mode: 0o600 });
  saveConfig({
    port: 0,
    defaultProvider: "anthropic",
    providers: {
      anthropic: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "oauth",
        headers: { "anthropic-beta": "provider-beta,source-beta" },
      },
    },
    anthropicAccountPool: { enabled: true, autoSwitchThreshold: 100 },
    combos: {
      waterfall: {
        strategy: "failover",
        targets: [{ provider: "anthropic", model: "claude-opus-5" }],
      },
    },
    claudeCode: { modelMap: { "claude-opus-5": "combo/waterfall" } },
  } as OcxConfig);
  const source = {
    model: "claude-opus-5",
    system: [{ type: "text", text: "system", cache_control: { type: "ephemeral", ttl: "1h" } }],
    messages: [{ role: "user", content: [{ type: "text", text: "count this" }] }],
    tools: [{ name: "Read", description: "read", input_schema: { type: "object", properties: {} } }],
    metadata: { user_id: "count-session" },
  };
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages/count_tokens", server.url), {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: "Bearer caller-admission",
        "x-api-key": "caller-admission",
        "anthropic-version": "2023-06-01",
        "anthropic-beta": "source-beta,duplicate-beta,source-beta",
        "user-agent": "source-user-agent",
        "x-arbitrary-source-header": "must-not-forward",
      },
      body: JSON.stringify(source),
    });
    expect(response.status).toBe(429);
    expect(await response.text()).toBe(upstreamBody);
    expect(response.headers.get("retry-after")).toBe("17");
    expect(captured).toHaveLength(1);
    expect(captured[0]!.body).toEqual(source);
    expect(captured[0]!.headers.get("authorization")).toBe("Bearer selected-count-token");
    expect(captured[0]!.headers.get("x-api-key")).toBeNull();
    expect(captured[0]!.headers.get("anthropic-version")).toBe("2023-06-01");
    expect(captured[0]!.headers.get("anthropic-beta")).toBe(
      "source-beta,duplicate-beta,provider-beta,claude-code-20250219,oauth-2025-04-20",
    );
    expect(captured[0]!.headers.get("user-agent")).not.toBe("source-user-agent");
    expect(captured[0]!.headers.get("x-arbitrary-source-header")).toBeNull();
    expect(captured[0]!.headers.get("connection")).toBe("close");
    expect(captured[0]!.keepalive).toBe(false);
    expect(getRequestLogEntries()).toHaveLength(0);
  } finally {
    await server.stop(true);
  }
});

test("count_tokens applies generation source eligibility before picking a combo target", async () => {
  const captured: Record<string, unknown>[] = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages/count_tokens") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    captured.push(await request.json() as Record<string, unknown>);
    return Response.json({ input_tokens: 123 });
  };
  const config = {
    port: 0,
    defaultProvider: "target",
    providers: {
      xai: {
        adapter: "openai-chat",
        baseUrl: "https://api.x.ai/v1",
        authMode: "key",
        apiKey: "xai-test-key",
      },
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    combos: {
      "count-source-waterfall": {
        strategy: "failover",
        targets: [
          { provider: "xai", model: "grok-4-1-fast" },
          { provider: "target", model: "claude-opus-5" },
          { provider: "target", model: "claude-haiku-4-5" },
        ],
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "combo/count-source-waterfall" } },
  } as OcxConfig;

  const response = await handleClaudeCountTokens(new Request("http://localhost/v1/messages/count_tokens", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(routedAnthropicNativeToolSource()),
  }), config);

  expect(response.status).toBe(200);
  expect(await response.json()).toEqual({ input_tokens: 123 });
  expect(captured).toEqual([{
    ...structuredClone(routedAnthropicNativeToolSource()),
    model: "claude-haiku-4-5",
  }]);
  expect(getRequestLogEntries()).toHaveLength(0);
});

test("generation preflight applies physical Anthropic combo eligibility", async () => {
  const captured: Record<string, unknown>[] = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    captured.push(await request.json() as Record<string, unknown>);
    return anthropicTextSse();
  };
  const config = {
    port: 0,
    defaultProvider: "estimated",
    providers: {
      estimated: {
        adapter: "kiro",
        baseUrl: "https://runtime.us-east-1.kiro.dev",
        authMode: "key",
        apiKey: "estimated-key",
      },
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    combos: {
      "source-preflight": {
        strategy: "failover",
        targets: [
          { provider: "estimated", model: "claude-sonnet-4.5" },
          { provider: "target", model: "claude-haiku-4-5" },
        ],
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "combo/source-preflight" } },
  } as OcxConfig;
  const logCtx: RequestLogContext = {};

  const response = await handleClaudeMessages(new Request("http://localhost/v1/messages", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(routedAnthropicNativeToolSource()),
  }), config, logCtx);

  expect(response.status).toBe(200);
  await response.text();
  expect(captured).toEqual([{
    ...structuredClone(routedAnthropicNativeToolSource()),
    model: "claude-haiku-4-5",
  }]);
  expect(logCtx.usageLogInputTokens).toBeUndefined();
});

test("count_tokens and following generation share a random combo target", async () => {
  const captured: Array<{ path: string; model: string }> = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    const body = await request.json() as { model: string };
    const path = new URL(request.url).pathname;
    captured.push({ path, model: body.model });
    if (path === "/v1/messages/count_tokens") {
      return Response.json({ input_tokens: 123 });
    }
    if (path !== "/v1/messages") throw new Error(`unexpected egress ${request.url}`);
    return new Response([
      `event: message_start\ndata: ${JSON.stringify({ type: "message_start", message: { id: "msg_random", type: "message", role: "assistant", model: body.model, content: [], stop_reason: null, usage: { input_tokens: 3, output_tokens: 0 } } })}\n\n`,
      'event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n',
      'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}\n\n',
      'event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n',
      'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":1}}\n\n',
      'event: message_stop\ndata: {"type":"message_stop"}\n\n',
    ].join(""), { headers: { "content-type": "text/event-stream" } });
  };
  const config = {
    port: 0,
    defaultProvider: "first",
    providers: {
      first: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "first-key",
      },
      second: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "second-key",
      },
    },
    combos: {
      "count-random": {
        strategy: "random",
        targets: [
          { provider: "first", model: "claude-opus-5" },
          { provider: "second", model: "claude-haiku-4-5" },
        ],
      },
    },
    claudeCode: { modelMap: { "claude-opus-5": "combo/count-random" } },
  } as OcxConfig;
  const source = {
    model: "claude-opus-5",
    metadata: { user_id: "random-count-generation-session" },
    messages: [{ role: "user", content: "same request" }],
  };
  const originalRandom = Math.random;
  const draws = [0, 0.99, 0.99];
  Math.random = () => draws.shift() ?? 0.99;
  try {
    const count = await handleClaudeCountTokens(new Request("http://localhost/v1/messages/count_tokens", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(source),
    }), config);
    expect(count.status).toBe(200);
    expect(await count.json()).toEqual({ input_tokens: 123 });

    const generation = await handleClaudeMessages(new Request("http://localhost/v1/messages", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ...source, max_tokens: 1024, stream: true }),
    }), config, {});
    expect(generation.status).toBe(200);
    expect(await generation.text()).toContain("message_stop");
    expect(captured.map(entry => entry.path)).toEqual([
      "/v1/messages/count_tokens",
      "/v1/messages",
    ]);
    expect(captured[1]!.model).toBe(captured[0]!.model);

    const changedSource = {
      ...source,
      metadata: { user_id: "random-count-generation-cooldown" },
      messages: [{ role: "user", content: "eligibility changes" }],
    };
    const changedCount = await handleClaudeCountTokens(new Request("http://localhost/v1/messages/count_tokens", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(changedSource),
    }), config);
    expect(changedCount.status).toBe(200);
    await changedCount.text();
    const countedModel = captured[2]!.model;
    const cooledProvider = countedModel === "claude-opus-5" ? "first" : "second";
    coolComboTarget("count-random", { provider: cooledProvider, model: countedModel }, { cooldownMs: 60_000 });
    try {
      const changedGeneration = await handleClaudeMessages(new Request("http://localhost/v1/messages", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ ...changedSource, max_tokens: 1024, stream: true }),
      }), config, {});
      expect(changedGeneration.status).toBe(200);
      await changedGeneration.text();
      expect(captured[3]!.model).not.toBe(countedModel);
    } finally {
      clearComboTargetCooldowns("count-random");
    }
  } finally {
    Math.random = originalRandom;
  }
});

test("count_tokens enforces combo image policy before dispatch", async () => {
  let sends = 0;
  globalThis.fetch = async () => {
    sends++;
    return Response.json({ input_tokens: 123 });
  };
  const config = {
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    combos: {
      "count-no-images": {
        strategy: "failover",
        imageInput: "disabled",
        targets: [{ provider: "target", model: "claude-opus-5" }],
      },
    },
    claudeCode: { modelMap: { "claude-opus-5": "combo/count-no-images" } },
  } as OcxConfig;

  const response = await handleClaudeCountTokens(new Request("http://localhost/v1/messages/count_tokens", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      model: "claude-opus-5",
      messages: [{
        role: "user",
        content: [{
          type: "image",
          source: {
            type: "base64",
            media_type: "image/png",
            data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==",
          },
        }],
      }],
    }),
  }), config);

  expect(response.status).toBe(400);
  expect(await response.text()).toContain("does not accept image input");
  expect(sends).toBe(0);
  expect(getRequestLogEntries()).toHaveLength(0);
});

test("count_tokens skips cooled combo targets like generation", async () => {
  const capturedModels: string[] = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages/count_tokens") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    capturedModels.push((await request.json() as { model: string }).model);
    return Response.json({ input_tokens: 321 });
  };
  const config = {
    port: 0,
    defaultProvider: "first",
    providers: {
      first: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "first-key",
      },
      backup: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "backup-key",
      },
    },
    combos: {
      "count-cooldown": {
        strategy: "failover",
        targets: [
          { provider: "first", model: "claude-opus-5" },
          { provider: "backup", model: "claude-haiku-4-5" },
        ],
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "combo/count-cooldown" } },
  } as OcxConfig;
  coolComboTarget(
    "count-cooldown",
    { provider: "first", model: "claude-opus-5" },
    { cooldownMs: 60_000 },
  );
  try {
    const response = await handleClaudeCountTokens(new Request("http://localhost/v1/messages/count_tokens", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-haiku-4-5",
        messages: [{ role: "user", content: "count this" }],
      }),
    }), config);

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ input_tokens: 321 });
    expect(capturedModels).toEqual(["claude-haiku-4-5"]);
    expect(getRequestLogEntries()).toHaveLength(0);
  } finally {
    clearComboTargetCooldowns("count-cooldown");
  }
});

test("mapped canonical count_tokens rejects translator overflow before dispatch", async () => {
  let sends = 0;
  globalThis.fetch = async () => {
    sends++;
    throw new Error("unexpected oversized count send");
  };
  writeFileSync(join(testDir, "auth.json"), JSON.stringify({
    anthropic: {
      activeAccountId: "overflow-account",
      accounts: [{
        id: "overflow-account",
        credential: {
          access: "selected-overflow-token",
          refresh: "test-refresh-token",
          expires: 9999999999999,
        },
      }],
    },
  }), { mode: 0o600 });
  saveConfig({
    port: 0,
    defaultProvider: "anthropic",
    providers: {
      anthropic: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "oauth",
      },
    },
    anthropicAccountPool: { enabled: true, autoSwitchThreshold: 100 },
    claudeCode: { modelMap: { "claude-opus-5": "anthropic/claude-opus-5" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages/count_tokens", server.url), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-opus-5",
        messages: [{ role: "user", content: "x".repeat(16 * 1024 * 1024) }],
      }),
    });
    expect(response.status).toBe(413);
    expect(await response.text()).toMatch(/translation_buffer_limit/);
    expect(sends).toBe(0);
  } finally {
    await server.stop(true);
  }
});

test("mapped canonical count_tokens cancels its physical request", async () => {
  let resolveStarted!: () => void;
  const started = new Promise<void>(resolve => { resolveStarted = resolve; });
  let physicalSignal: AbortSignal | null = null;
  globalThis.fetch = async (_input, init) => {
    const signal = init?.signal;
    physicalSignal = signal ?? null;
    resolveStarted();
    return await new Promise<Response>((_resolve, reject) => {
      if (!signal) return reject(new Error("missing physical abort signal"));
      signal.addEventListener("abort", () => reject(signal.reason), { once: true });
    });
  };
  const config = {
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    claudeCode: { modelMap: { "claude-opus-5": "target/claude-opus-5" } },
  } as OcxConfig;
  const client = new AbortController();
  const pending = handleClaudeCountTokens(new Request("http://localhost/v1/messages/count_tokens", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      model: "claude-opus-5",
      messages: [{ role: "user", content: "count this" }],
    }),
    signal: client.signal,
  }), config);
  await started;
  client.abort(new DOMException("client closed", "AbortError"));
  expect((await pending).status).toBe(499);
  expect(physicalSignal?.aborted).toBe(true);
  expect(getRequestLogEntries()).toHaveLength(0);
});

test("mapped noncanonical count_tokens remains local", async () => {
  let sends = 0;
  globalThis.fetch = async () => {
    sends++;
    throw new Error("unexpected noncanonical count send");
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://compatible.example",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    claudeCode: { modelMap: { "claude-opus-5": "target/claude-opus-5" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages/count_tokens", server.url), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "claude-opus-5",
        messages: [{ role: "user", content: "count locally" }],
      }),
    });
    expect(response.status).toBe(200);
    expect((await response.json() as { input_tokens: number }).input_tokens).toBeGreaterThan(0);
    expect(sends).toBe(0);
  } finally {
    await server.stop(true);
  }
});

test("incompatible canonical count_tokens fails before dispatch", async () => {
  let sends = 0;
  globalThis.fetch = async () => {
    sends++;
    throw new Error("unexpected incompatible count send");
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "target/claude-opus-5" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages/count_tokens", server.url), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(routedAnthropicNativeToolSource()),
    });
    expect(response.status).toBe(400);
    expect(await response.text()).toMatch(/replayed exactly/);
    expect(sends).toBe(0);
    expect(getRequestLogEntries()).toHaveLength(0);
  } finally {
    await server.stop(true);
  }
});

/** Minimal PNG header (signature + IHDR) so the attachment sniffer can read real dimensions. */
function countTokensPngBase64(width: number, height: number): string {
  const u32be = (n: number): number[] => [(n >>> 24) & 0xff, (n >>> 16) & 0xff, (n >>> 8) & 0xff, n & 0xff];
  const bytes = [
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
    ...u32be(13), 0x49, 0x48, 0x44, 0x52, // len + "IHDR"
    ...u32be(width), ...u32be(height),
    8, 6, 0, 0, 0, // bit depth, color type, etc.
  ];
  return Buffer.from(Uint8Array.from(bytes)).toString("base64");
}

test("count_tokens prices base64 attachments as attachments, not characters", async () => {
  saveConfig(mockConfig("http://127.0.0.1:1/v1"));
  const server = startServer(0);
  try {
    const data = "A".repeat(700_000); // ~512KB decoded; counting chars would report ~200k tokens
    const response = await fetch(new URL("/v1/messages/count_tokens", server.url), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "mock/test-model",
        messages: [{
          role: "user",
          content: [
            { type: "text", text: "what is in this screenshot?" },
            { type: "image", source: { type: "base64", media_type: "image/png", data } },
          ],
        }],
      }),
    });
    expect(response.status).toBe(200);
    const json = await response.json() as { input_tokens: number };
    // ceil(700000 * 3/4 / 512) = 1026 attachment tokens plus a small text remainder.
    expect(json.input_tokens).toBeGreaterThanOrEqual(1026);
    expect(json.input_tokens).toBeLessThan(2000);
  } finally {
    await server.stop(true);
  }
});

test("estimateClaudeRequestTokens matches the plain char estimate for text-only bodies", () => {
  const raw = {
    system: "be brief",
    messages: [{ role: "user", content: "count me please, this is a sentence" }],
    tools: [{ name: "Read", input_schema: { type: "object" } }],
  };
  const parts = [raw.system, JSON.stringify(raw.messages), JSON.stringify(raw.tools)];
  expect(estimateClaudeRequestTokens(raw, "m")).toBe(Math.max(1, estimateTokens(parts.join("\n"), "m")));
});

test("estimateClaudeRequestTokens prices sniffable images by pixel dimensions", () => {
  const raw = {
    messages: [{
      role: "user",
      content: [{ type: "image", source: { type: "base64", media_type: "image/png", data: countTokensPngBase64(1500, 2000) } }],
    }],
  };
  const estimate = estimateClaudeRequestTokens(raw, "m");
  // ceil(1500 * 2000 / 750) = 4000 attachment tokens plus the JSON skeleton.
  expect(estimate).toBeGreaterThanOrEqual(4000);
  expect(estimate).toBeLessThan(4100);
});

test("estimateClaudeRequestTokens strips base64 documents nested in tool_result content", () => {
  const raw = {
    messages: [{
      role: "user",
      content: [{
        type: "tool_result",
        tool_use_id: "t1",
        content: [{ type: "document", source: { type: "base64", media_type: "application/pdf", data: "Q".repeat(400_000) } }],
      }],
    }],
  };
  const estimate = estimateClaudeRequestTokens(raw, "m");
  // ceil(400000 * 3/4 / 512) = 586 tokens, nowhere near the ~114k a char count would report.
  expect(estimate).toBeGreaterThanOrEqual(586);
  expect(estimate).toBeLessThan(1000);
});

test("estimateClaudeRequestTokens does not charge base64 padding as payload bytes", () => {
  // Exactly 131072 decoded bytes: 174764 base64 chars ending in "=". Counting the padding
  // would yield 131073 bytes and charge 257 tokens instead of 256.
  const data = Buffer.from(new Uint8Array(131_072)).toString("base64");
  const raw = {
    messages: [{ role: "user", content: [{ type: "image", source: { type: "base64", media_type: "image/png", data } }] }],
  };
  const stripped = {
    messages: [{ role: "user", content: [{ type: "image", source: { type: "base64", media_type: "image/png", data: "" } }] }],
  };

  expect(estimateClaudeRequestTokens(raw, "m")).toBe(
    Math.max(1, estimateTokens(JSON.stringify(stripped.messages), "m") + 256),
  );
});

test("estimateClaudeRequestTokens keeps base64-shaped tool_use input counted as text", () => {
  // tool_use.input is serialized into function_call arguments and sent upstream, so a
  // {type:"base64", data} shape inside it is NOT an attachment and must count as text.
  const raw = {
    messages: [{
      role: "assistant",
      content: [{ type: "tool_use", id: "t1", name: "upload", input: { type: "base64", data: "B".repeat(40_000) } }],
    }],
  };

  expect(estimateClaudeRequestTokens(raw, "m")).toBe(
    Math.max(1, estimateTokens(JSON.stringify(raw.messages), "m")),
  );
});

test("estimateClaudeRequestTokens leaves complete attachment-shaped tool_use input intact", () => {
  // Even a full {type:"image", source:{type:"base64", data}} object inside tool_use.input
  // is a tool argument, not an attachment: the translator replays it verbatim inside
  // function_call arguments, so it must count at its serialized size.
  const raw = {
    messages: [{
      role: "assistant",
      content: [{
        type: "tool_use",
        id: "t1",
        name: "upload_image",
        input: { type: "image", source: { type: "base64", media_type: "image/png", data: "C".repeat(50_000) } },
      }],
    }],
  };

  expect(estimateClaudeRequestTokens(raw, "m")).toBe(
    Math.max(1, estimateTokens(JSON.stringify(raw.messages), "m")),
  );
});

test("estimateClaudeRequestTokens leaves attachment-shaped tool schemas intact", () => {
  // Tool definitions are forwarded to routed providers; an attachment-shaped example in a
  // schema is not an attachment either.
  const raw = {
    messages: [{ role: "user", content: "hi" }],
    tools: [{
      name: "upload",
      input_schema: { type: "object" },
      example: { type: "image", source: { type: "base64", media_type: "image/png", data: "D".repeat(30_000) } },
    }],
  };
  const parts = [JSON.stringify(raw.messages), JSON.stringify(raw.tools)];

  expect(estimateClaudeRequestTokens(raw, "m")).toBe(
    Math.max(1, estimateTokens(parts.join("\n"), "m")),
  );
});

test("estimateClaudeRequestTokens counts text-source documents as ordinary text", () => {
  const text = "plain text document body ".repeat(40);
  const raw = {
    messages: [{
      role: "user",
      content: [{ type: "document", source: { type: "text", media_type: "text/plain", data: text } }],
    }],
  };
  expect(estimateClaudeRequestTokens(raw, "m")).toBe(Math.max(1, estimateTokens(JSON.stringify(raw.messages), "m")));
});

test("claudeCode.enabled=false -> 403 permission_error on both routes", async () => {
  saveConfig(mockConfig("http://127.0.0.1:1/v1", { enabled: false }));
  const server = startServer(0);
  try {
    for (const path of ["/v1/messages", "/v1/messages/count_tokens"]) {
      const response = await fetch(new URL(path, server.url), {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ model: "m", max_tokens: 5, messages: [{ role: "user", content: "x" }] }),
      });
      expect(response.status).toBe(403);
      const json = await response.json() as Record<string, any>;
      expect(json.error.type).toBe("permission_error");
    }
  } finally {
    await server.stop(true);
  }
});

async function postMessages(serverUrl: string, body: Record<string, unknown>): Promise<Response> {
  return fetch(new URL("/v1/messages", serverUrl), {
    method: "POST",
    headers: { "content-type": "application/json", "x-api-key": "placeholder", "anthropic-version": "2023-06-01" },
    body: JSON.stringify(body),
  });
}

test("effort safety valve: routes with a definitive no-effort ladder get reasoning stripped (devlog 136 B6)", async () => {
  const { server: upstream, captured } = mockChatUpstreamCapturing();
  const base = `${upstream.url.toString().replace(/\/$/, "")}/v1`;
  const config = mockConfig(base);
  (config.providers.mock as Record<string, unknown>).noReasoningModels = ["test-model"];
  saveConfig(config);
  const server = startServer(0);
  try {
    const response = await postMessages(server.url.toString(), {
      model: "mock/test-model",
      max_tokens: 64,
      stream: true,
      thinking: { type: "adaptive" },
      output_config: { effort: "low" },
      messages: [{ role: "user", content: "hi" }],
    });
    expect(response.status).toBe(200);
    await response.text();
    expect(captured.length).toBe(1);
    expect(captured[0]!.reasoning_effort).toBeUndefined();
  } finally {
    await server.stop(true);
    upstream.stop(true);
  }
});

test("generated agent effort directive restores exact xhigh and max after Claude Code collapses them to a thinking budget", async () => {
  const { server: upstream, captured } = mockChatUpstreamCapturing();
  saveConfig(mockConfig(`${upstream.url.toString().replace(/\/$/, "")}/v1`));
  const server = startServer(0);
  try {
    for (const effort of ["xhigh", "max"]) {
      const response = await postMessages(server.url.toString(), {
        model: "claude-haiku-4-5",
        max_tokens: 32000,
        stream: true,
        system: [
          { type: "text", text: "<!-- ocx-route: claude-ocx-mock--test-model -->" },
          { type: "text", text: `<!-- ocx-effort: ${effort} -->` },
        ],
        thinking: { type: "enabled", budget_tokens: 31999 },
        messages: [{ role: "user", content: "hi" }],
      });
      expect(response.status).toBe(200);
      await response.text();
    }
    expect(captured.map(body => ({ model: body.model, effort: body.reasoning_effort }))).toEqual([
      { model: "test-model", effort: "xhigh" },
      { model: "test-model", effort: "max" },
    ]);
  } finally {
    await server.stop(true);
    upstream.stop(true);
  }
});

function routedAnthropicSource(): Record<string, unknown> {
  return {
    model: "claude-haiku-4-5",
    max_tokens: 64000,
    stream: true,
    system: [
      { type: "text", text: "identity" },
      { type: "text", text: "policy", cache_control: { type: "ephemeral", ttl: "1h" } },
      { type: "text", text: "tools", cache_control: { type: "ephemeral" } },
    ],
    messages: [
      { role: "user", content: [{ type: "text", text: "first" }] },
      {
        role: "assistant",
        content: [
          { type: "redacted_thinking", data: "opaque-source-data" },
          { type: "thinking", thinking: "private thought", signature: "source-signature-1234567890" },
          { type: "text", text: "calling" },
          { type: "tool_use", id: "toolu_source", name: "custom_source", input: { value: 1 } },
        ],
      },
      {
        role: "user",
        content: [{
          type: "tool_result",
          tool_use_id: "toolu_source",
          content: [{ type: "text", text: "done" }],
          cache_control: { type: "ephemeral" },
        }],
      },
      {
        role: "user",
        content: [{ type: "text", text: "continue" }],
      },
    ],
    tools: [{
      name: "custom_source",
      description: "source tool",
      input_schema: { type: "object", properties: { value: { type: "number" } }, required: ["value"] },
    }],
    thinking: { type: "adaptive", display: "omitted" },
    output_config: { effort: "high" },
    context_management: { source_sentinel: true },
    metadata: { user_id: "source-session" },
  };
}

function routedAnthropicNativeToolSource(): Record<string, any> {
  return {
    model: "claude-haiku-4-5",
    max_tokens: 128,
    stream: true,
    messages: [{ role: "user", content: [{ type: "text", text: "use the computer" }] }],
    tools: [{
      type: "computer_20250124",
      name: "computer",
      display_width_px: 1024,
      display_height_px: 768,
      input_schema: { type: "object" },
    }],
    context_management: { source_sentinel: true },
  };
}

function anthropicTextSse(text = "ok", stopReason = "end_turn"): Response {
  return new Response([
    'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_test","type":"message","role":"assistant","model":"claude-opus-5","content":[],"stop_reason":null,"usage":{"input_tokens":3,"output_tokens":0}}}\n\n',
    `event: content_block_start\ndata: ${JSON.stringify({ type: "content_block_start", index: 0, content_block: { type: "text", text: "" } })}\n\n`,
    `event: content_block_delta\ndata: ${JSON.stringify({ type: "content_block_delta", index: 0, delta: { type: "text_delta", text } })}\n\n`,
    'event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n',
    `event: message_delta\ndata: ${JSON.stringify({ type: "message_delta", delta: { stop_reason: stopReason }, usage: { output_tokens: 1 } })}\n\n`,
    'event: message_stop\ndata: {"type":"message_stop"}\n\n',
  ].join(""), { headers: { "content-type": "text/event-stream" } });
}

function anthropicTextJson(text = "ok", citations?: Array<Record<string, unknown>>): Response {
  return Response.json({
    id: "msg_test",
    type: "message",
    role: "assistant",
    model: "claude-opus-5",
    content: [{ type: "text", text, ...(citations ? { citations } : {}) }],
    stop_reason: "end_turn",
    stop_sequence: null,
    usage: { input_tokens: 3, output_tokens: 1 },
  });
}

function anthropicCitationSse(citations: Array<Record<string, unknown>>): Response {
  return new Response([
    'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_citations","type":"message","role":"assistant","model":"claude-opus-5","content":[],"stop_reason":null,"usage":{"input_tokens":8,"output_tokens":0}}}\n\n',
    'event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n',
    'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"A"}}\n\n',
    `event: content_block_delta\ndata: ${JSON.stringify({ type: "content_block_delta", index: 0, delta: { type: "citations_delta", citation: citations[0] } })}\n\n`,
    'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"B"}}\n\n',
    `event: content_block_delta\ndata: ${JSON.stringify({ type: "content_block_delta", index: 0, delta: { type: "citations_delta", citation: citations[1] } })}\n\n`,
    'event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n',
    'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":2}}\n\n',
    'event: message_stop\ndata: {"type":"message_stop"}\n\n',
  ].join(""), { headers: { "content-type": "text/event-stream" } });
}

test("routed canonical Anthropic combo relays successful SSE and JSON bytes unchanged", async () => {
  clearRequestLogsForTests();
  const exactSse = sseEncoder.encode([
    'event:message_start\ndata:{"type":"message_start","message":{"id":"msg_raw","type":"message","role":"assistant","model":"claude-opus-5","content":[],"stop_reason":null,"usage":{"input_tokens":3,"cache_creation_input_tokens":5,"cache_read_input_tokens":7,"output_tokens":0}}}\n\n',
    'event:content_block_start\ndata:{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n',
    'event:content_block_delta\ndata:{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"héllo"}}\n\n',
    'event:content_block_stop\ndata:{"type":"content_block_stop","index":0}\n\n',
    'event:message_delta\ndata:{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}\n\n',
    'event:message_stop\ndata:{"type":"message_stop"}\n\n',
  ].join(""));
  const exactJson = sseEncoder.encode([
    " {\n",
    '  "id" : "msg_raw_json", "type" : "message", "role" : "assistant",\n',
    '  "model" : "claude-opus-5", "content" : [{"type":"text","text":"héllo"}],\n',
    '  "stop_reason" : "end_turn", "stop_sequence" : null,\n',
    '  "usage" : {"input_tokens":2,"cache_creation_input_tokens":13,"cache_read_input_tokens":11,"output_tokens":1}\n',
    " }\n",
  ].join(""));
  const physicalStreams: boolean[] = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    const body = await request.json() as { stream?: boolean };
    physicalStreams.push(body.stream === true);
    if (body.stream === true) {
      return new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(exactSse.slice(0, 19));
          controller.enqueue(exactSse.slice(19, 173));
          controller.enqueue(exactSse.slice(173));
          controller.close();
        },
      }), { headers: { "content-type": "text/event-stream", "x-upstream-proof": "sse" } });
    }
    return new Response(Uint8Array.from(exactJson).buffer, {
      headers: { "content-type": "application/json", "x-upstream-proof": "json" },
    });
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    combos: {
      waterfall: {
        strategy: "failover",
        targets: [{ provider: "target", model: "claude-opus-5" }],
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "combo/waterfall" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const streamingResponse = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify({ ...routedAnthropicSource(), stream: true }),
    });
    expect(streamingResponse.status).toBe(200);
    expect(streamingResponse.headers.get("x-upstream-proof")).toBe("sse");
    expect(new Uint8Array(await streamingResponse.arrayBuffer())).toEqual(exactSse);
    const streamingEntry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(streamingEntry?.attempts).toMatchObject([{
      provider: "target",
      model: "claude-opus-5",
      sendCount: 1,
      recoveryKinds: [],
      usage: {
        inputTokens: 15,
        outputTokens: 1,
        cachedInputTokens: 7,
        cacheReadInputTokens: 7,
        cacheCreationInputTokens: 5,
      },
    }]);

    const jsonResponse = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify({ ...routedAnthropicSource(), stream: false }),
    });
    expect(jsonResponse.status).toBe(200);
    expect(jsonResponse.headers.get("x-upstream-proof")).toBe("json");
    expect(new Uint8Array(await jsonResponse.arrayBuffer())).toEqual(exactJson);
    const jsonEntry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(jsonEntry?.attempts).toMatchObject([{
      provider: "target",
      model: "claude-opus-5",
      sendCount: 1,
      recoveryKinds: [],
      usage: {
        inputTokens: 26,
        outputTokens: 1,
        cachedInputTokens: 11,
        cacheReadInputTokens: 11,
        cacheCreationInputTokens: 13,
      },
    }]);
    expect(physicalStreams).toEqual([true, false]);
  } finally {
    await server.stop(true);
    clearRequestLogsForTests();
  }
});

test("streamed direct canonical replay returns an Anthropic error for 200 JSON", async () => {
  let sends = 0;
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    expect(request.url).toBe("https://api.anthropic.com/v1/messages");
    expect((await request.json() as { stream?: boolean }).stream).toBe(true);
    sends += 1;
    return anthropicTextJson("invalid transport");
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "target/claude-opus-5" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify({ ...routedAnthropicSource(), stream: true }),
    });
    expect(response.status).toBe(502);
    expect(await response.json()).toEqual({
      type: "error",
      error: {
        type: "api_error",
        message: "canonical Anthropic returned JSON for a streaming request",
      },
    });
    expect(sends).toBe(1);
  } finally {
    await server.stop(true);
  }
});

test("direct canonical nonstream rejects invalid UTF-8 before relaying", async () => {
  const prefix = sseEncoder.encode(
    '{"id":"msg_test","type":"message","role":"assistant","model":"claude-opus-5","content":[{"type":"text","text":"',
  );
  const suffix = sseEncoder.encode(
    '"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":1}}',
  );
  const invalidJson = Uint8Array.from([
    ...prefix,
    0xc3,
    0x28,
    ...suffix,
  ]);
  let sends = 0;
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    expect(request.url).toBe("https://api.anthropic.com/v1/messages");
    expect((await request.json() as { stream?: boolean }).stream).toBe(false);
    sends += 1;
    return new Response(invalidJson, {
      headers: { "content-type": "application/json" },
    });
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "target/claude-opus-5" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify({ ...routedAnthropicSource(), stream: false }),
    });
    expect(response.status).toBe(502);
    expect(await response.json()).toEqual({
      type: "error",
      error: {
        type: "api_error",
        message: "canonical Anthropic response was not valid JSON",
      },
    });
    expect(sends).toBe(1);
  } finally {
    await server.stop(true);
  }
});

test.each([
  {
    name: "configured body overflow",
    response: () => Response.json({
      id: "msg_test",
      type: "message",
      role: "assistant",
      model: "claude-opus-5",
      content: [{ type: "text", text: "x".repeat(512) }],
      stop_reason: "end_turn",
      stop_sequence: null,
      usage: { input_tokens: 3, output_tokens: 1 },
    }),
    message: "canonical Anthropic response exceeded the configured body limit",
  },
  {
    name: "body read failure",
    response: () => new Response(new ReadableStream<Uint8Array>({
      pull(controller) { controller.error(new Error("wire broke")); },
    }), { headers: { "content-type": "application/json" } }),
    message: "canonical Anthropic response was not valid JSON",
  },
])("direct canonical nonstream rejects $name", async ({ response: upstreamResponse, message }) => {
  let sends = 0;
  globalThis.fetch = async () => {
    sends += 1;
    return upstreamResponse();
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    claudeCode: {
      bodyMaxBytes: 256,
      modelMap: { "claude-haiku-4-5": "target/claude-opus-5" },
    },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify({ ...routedAnthropicSource(), stream: false }),
    });
    expect(response.status).toBe(502);
    expect(await response.json()).toEqual({
      type: "error",
      error: { type: "api_error", message },
    });
    expect(sends).toBe(1);
  } finally {
    await server.stop(true);
  }
});

test("direct canonical nonstream rejects incomplete Anthropic message envelopes", async () => {
  let sends = 0;
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    expect(request.url).toBe("https://api.anthropic.com/v1/messages");
    expect((await request.json() as { stream?: boolean }).stream).toBe(false);
    sends += 1;
    return Response.json({ type: "message" });
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "target/claude-opus-5" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify({ ...routedAnthropicSource(), stream: false }),
    });
    expect(response.status).toBe(502);
    expect(await response.json()).toEqual({
      type: "error",
      error: {
        type: "api_error",
        message: "canonical Anthropic returned an invalid JSON response",
      },
    });
    expect(sends).toBe(1);
  } finally {
    await server.stop(true);
  }
});

test("routed canonical Anthropic relays final JSON errors byte-for-byte for streaming and buffered clients", async () => {
  const exactError = sseEncoder.encode(' {"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}\n');
  let sends = 0;
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    sends++;
    return new Response(Uint8Array.from(exactError).buffer, {
      status: 429,
      statusText: "Rate Limited",
      headers: {
        "content-type": "application/json",
        "retry-after": "17",
      },
    });
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    combos: {
      "final-error-streaming": {
        strategy: "failover",
        targets: [{ provider: "target", model: "claude-opus-5" }],
      },
      "final-error-buffered": {
        strategy: "failover",
        targets: [{ provider: "target", model: "claude-opus-5" }],
      },
    },
    claudeCode: {
      modelMap: {
        "claude-haiku-4-5": "target/claude-opus-5",
        "claude-sonnet-4-5": "combo/final-error-streaming",
        "claude-opus-4-6": "combo/final-error-buffered",
      },
    },
  } as OcxConfig);
  const server = startServer(0);
  try {
    for (const { model, stream } of [
      { model: "claude-haiku-4-5", stream: true },
      { model: "claude-haiku-4-5", stream: false },
      { model: "claude-sonnet-4-5", stream: true },
      { model: "claude-opus-4-6", stream: false },
    ]) {
      const response = await originalFetch(new URL("/v1/messages", server.url), {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": "placeholder" },
        body: JSON.stringify({ ...routedAnthropicSource(), model, stream }),
      });
      expect(response.status).toBe(429);
      expect(response.headers.get("retry-after")).toBe("17");
      expect(new Uint8Array(await response.arrayBuffer())).toEqual(exactError);
    }
    expect(sends).toBe(4);
  } finally {
    await server.stop(true);
  }
});

test("routed canonical source preserves large, opaque, and streaming 413 error bytes", async () => {
  const large = sseEncoder.encode(JSON.stringify({
    type: "error",
    error: { type: "invalid_request_error", message: "x".repeat(70 * 1024) },
  }));
  const malformed = Uint8Array.from([
    ...sseEncoder.encode('{"type":"error","error":{"type":"api_error","message":"'),
    0xc3,
    0x28,
    ...sseEncoder.encode('"}}'),
  ]);
  const overflow = sseEncoder.encode(
    ' {"type":"error","error":{"type":"request_too_large","message":"source overflow"}}\n',
  );
  const cases = new Map<number, { status: number; bytes: Uint8Array }>([
    [103, { status: 413, bytes: overflow }],
    [101, { status: 400, bytes: large }],
    [102, { status: 500, bytes: malformed }],
  ]);
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    expect(request.url).toBe("https://api.anthropic.com/v1/messages");
    const body = await request.json() as { max_tokens?: number };
    const testCase = cases.get(body.max_tokens ?? 0);
    if (!testCase) throw new Error(`unexpected source max_tokens ${body.max_tokens}`);
    return new Response(Uint8Array.from(testCase.bytes).buffer, {
      status: testCase.status,
      headers: { "content-type": "application/json", "retry-after": "19" },
    });
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    combos: {
      "source-error-bytes": {
        strategy: "failover",
        targets: [{ provider: "target", model: "claude-opus-5" }],
      },
    },
    claudeCode: {
      modelMap: {
        "claude-haiku-4-5": "combo/source-error-bytes",
        "claude-sonnet-4-5": "combo/source-error-bytes",
        "claude-opus-4-6": "combo/source-error-bytes",
      },
    },
  } as OcxConfig);
  const server = startServer(0);
  try {
    for (const [index, [maxTokens, testCase]] of [...cases].entries()) {
      const model = ["claude-haiku-4-5", "claude-sonnet-4-5", "claude-opus-4-6"][index]!;
      const response = await originalFetch(new URL("/v1/messages", server.url), {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": "placeholder" },
        body: JSON.stringify({ ...routedAnthropicSource(), model, max_tokens: maxTokens, stream: true }),
      });
      expect(response.status).toBe(testCase.status);
      expect(response.headers.get("retry-after")).toBe("19");
      expect(new Uint8Array(await response.arrayBuffer())).toEqual(testCase.bytes);
    }
  } finally {
    await server.stop(true);
  }
});

test("combo preserves final HTTP 200 Anthropic terminal errors byte-for-byte", async () => {
  clearRequestLogsForTests();
  const exactSse = sseEncoder.encode([
    'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_failed","type":"message","role":"assistant","model":"claude-opus-5","content":[],"stop_reason":null,"usage":{"input_tokens":3,"cache_creation_input_tokens":5,"cache_read_input_tokens":7,"output_tokens":0}}}\n\n',
    'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":null},"usage":{"output_tokens":2}}\n\n',
    'event: error\ndata: {"type":"error","error":{"type":"overloaded_error","message":"busy"}}\n\n',
  ].join(""));
  const exactJson = sseEncoder.encode(
    ' {"type":"error","error":{"type":"overloaded_error","message":"busy"}}\n',
  );
  let sends = 0;
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    sends += 1;
    const body = await request.json() as { stream?: boolean };
    return new Response(
      Uint8Array.from(body.stream ? exactSse : exactJson).buffer,
      {
        headers: {
          "content-type": body.stream
            ? "text/event-stream"
            : "application/json",
          "x-source-error": "preserved",
        },
      },
    );
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    combos: {
      "terminal-error-stream": {
        strategy: "failover",
        targets: [{ provider: "target", model: "claude-opus-5" }],
      },
      "terminal-error-json": {
        strategy: "failover",
        targets: [{ provider: "target", model: "claude-opus-5" }],
      },
    },
    claudeCode: {
      modelMap: {
        "claude-sonnet-4-5": "combo/terminal-error-stream",
        "claude-opus-4-6": "combo/terminal-error-json",
      },
    },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const streaming = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify({
        ...routedAnthropicSource(),
        model: "claude-sonnet-4-5",
        stream: true,
      }),
    });
    expect(streaming.status).toBe(200);
    expect(streaming.headers.get("x-source-error")).toBe("preserved");
    expect(new Uint8Array(await streaming.arrayBuffer())).toEqual(exactSse);
    expect(getRequestLogEntries().findLast(row => row.surface === "claude")?.attempts).toMatchObject([{
      provider: "target",
      sendCount: 1,
      usage: {
        inputTokens: 15,
        outputTokens: 2,
        cachedInputTokens: 7,
        cacheReadInputTokens: 7,
        cacheCreationInputTokens: 5,
      },
    }]);

    const buffered = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify({
        ...routedAnthropicSource(),
        model: "claude-opus-4-6",
        stream: false,
      }),
    });
    expect(buffered.status).toBe(529);
    expect(buffered.headers.get("x-source-error")).toBe("preserved");
    expect(new Uint8Array(await buffered.arrayBuffer())).toEqual(exactJson);
    expect(sends).toBe(2);
  } finally {
    await server.stop(true);
    clearRequestLogsForTests();
  }
});

test("routed canonical Anthropic preserves citations for streaming and buffered clients", async () => {
  const citations = [
    { type: "char_location", cited_text: "first", document_index: 0, start_char_index: 4, end_char_index: 9 },
    { type: "web_search_result_location", cited_text: "second", url: "https://example.test/source", title: "Source" },
  ];
  let sends = 0;
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    sends++;
    const body = await request.json() as { stream?: boolean };
    return body.stream ? anthropicCitationSse(citations) : anthropicTextJson("AB", citations);
  };
  saveConfig({
    port: 0,
    defaultProvider: "citation-target",
    providers: {
      "citation-target": {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    claudeCode: {
      modelMap: { "claude-haiku-4-5": "citation-target/claude-opus-5" },
    },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const streamingSource = { ...routedAnthropicSource(), stream: true };
    const streamingResponse = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(streamingSource),
    });
    expect(streamingResponse.status).toBe(200);
    const frames = (await streamingResponse.text())
      .split("\n")
      .filter(line => line.startsWith("data: "))
      .map(line => JSON.parse(line.slice(6)) as Record<string, any>);
    expect(frames
      .filter(frame => frame.type === "content_block_delta")
      .map(frame => frame.delta)).toEqual([
      { type: "text_delta", text: "A" },
      { type: "citations_delta", citation: citations[0] },
      { type: "text_delta", text: "B" },
      { type: "citations_delta", citation: citations[1] },
    ]);

    const bufferedSource = { ...routedAnthropicSource(), stream: false };
    const bufferedResponse = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(bufferedSource),
    });
    expect(bufferedResponse.status).toBe(200);
    const buffered = await bufferedResponse.json() as { content: Array<Record<string, unknown>> };
    expect(buffered.content).toContainEqual({ type: "text", text: "AB", citations });
    expect(sends).toBe(2);
  } finally {
    await server.stop(true);
  }
});

test("routed canonical Anthropic preserves the validated Messages source wire", async () => {
  clearRequestLogsForTests();
  const captured: Array<{
    url: string;
    headers: Headers;
    body: Record<string, unknown>;
    keepalive?: boolean;
  }> = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    captured.push({
      url: request.url,
      headers: request.headers,
      body: await request.json() as Record<string, unknown>,
      keepalive: init?.keepalive,
    });
    return new Response([
      'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_test","type":"message","role":"assistant","model":"claude-opus-5","content":[],"stop_reason":null,"usage":{"input_tokens":3,"cache_creation_input_tokens":5,"cache_read_input_tokens":7,"output_tokens":0}}}\n\n',
      'event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_reply","name":"custom_source","input":{}}}\n\n',
      'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{}"}}\n\n',
      'event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n',
      'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}}\n\n',
      'event: message_stop\ndata: {"type":"message_stop"}\n\n',
    ].join(""), { headers: { "content-type": "text/event-stream" } });
  };
  writeFileSync(join(testDir, "auth.json"), JSON.stringify({
    anthropic: {
      activeAccountId: "test-account",
      accounts: [{
        id: "test-account",
        credential: {
          access: "selected-oauth-token",
          refresh: "test-refresh-token",
          expires: 9999999999999,
        },
      }],
    },
  }), { mode: 0o600 });
  saveConfig({
    port: 0,
    defaultProvider: "anthropic",
    providers: {
      anthropic: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "oauth",
        headers: { "anthropic-beta": "provider-beta,source-beta" },
      },
    },
    claudeCode: {
      modelMap: { "claude-haiku-4-5": "anthropic/claude-opus-5" },
    },
  } as OcxConfig);
  const source = routedAnthropicSource();
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": "placeholder",
        "anthropic-version": "2023-06-01",
        "anthropic-beta": "source-beta,duplicate-beta,source-beta",
        "user-agent": "source-user-agent",
        "x-client-request-id": "source-request-id",
        "x-arbitrary-source-header": "must-not-forward",
      },
      body: JSON.stringify(source),
    });
    if (!response.ok) throw new Error(`${response.status}: ${await response.clone().text()} (captured=${captured.length})`);
    expect(response.status).toBe(200);
    const result = await response.text();
    expect(captured).toHaveLength(1);
    expect(captured[0]!.body).toEqual({ ...structuredClone(source), model: "claude-opus-5" });
    expect(captured[0]!.headers.get("authorization")).toBe("Bearer selected-oauth-token");
    expect(captured[0]!.headers.get("x-api-key")).toBeNull();
    expect(captured[0]!.headers.get("anthropic-version")).toBe("2023-06-01");
    expect(captured[0]!.headers.get("user-agent")).not.toBe("source-user-agent");
    expect(captured[0]!.headers.get("x-client-request-id")).not.toBe("source-request-id");
    expect(captured[0]!.headers.get("x-arbitrary-source-header")).toBeNull();
    expect(captured[0]!.headers.get("connection")).toBe("close");
    expect(captured[0]!.keepalive).toBe(false);
    expect(captured[0]!.headers.get("anthropic-beta")).toBe(
      "source-beta,duplicate-beta,provider-beta,claude-code-20250219,oauth-2025-04-20",
    );
    expect(result).toContain('"name":"custom_source"');
    expect(getRequestLogEntries().findLast(row => row.surface === "claude")?.attempts).toMatchObject([
      { sendCount: 1, recoveryKinds: [] },
    ]);
  } finally {
    await server.stop(true);
  }
});

test("combo replays current Claude Code system-role history to canonical Anthropic", async () => {
  clearRequestLogsForTests();
  const captured: Record<string, unknown>[] = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    captured.push(await request.json() as Record<string, unknown>);
    return anthropicTextSse();
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    combos: {
      waterfall: {
        strategy: "failover",
        targets: [{ provider: "target", model: "claude-opus-5" }],
      },
    },
  } as OcxConfig);
  const source = {
    ...routedAnthropicSource(),
    model: "combo/waterfall",
    max_tokens: 32_000,
    output_config: { effort: "low" },
    messages: [
      { role: "user", content: [{ type: "text", text: "use Read once" }] },
      { role: "system", content: [{ type: "text", text: "synthetic reminder" }] },
    ],
    tools: [{
      name: "Read",
      description: "read a file",
      input_schema: { type: "object", properties: { file_path: { type: "string" } } },
    }],
  };
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(source),
    });
    expect(response.status).toBe(200);
    await response.text();
    expect(captured).toEqual([{ ...structuredClone(source), model: "claude-opus-5" }]);
    expect(getRequestLogEntries().findLast(row => row.surface === "claude")?.attempts).toHaveLength(1);
  } finally {
    await server.stop(true);
    clearRequestLogsForTests();
  }
});

test("streamed source replay fails over before output when a canonical target returns 200 JSON", async () => {
  clearRequestLogsForTests();
  let sends = 0;
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    expect(request.url).toBe("https://api.anthropic.com/v1/messages");
    await request.body?.cancel();
    sends += 1;
    return sends === 1 ? anthropicTextJson("invalid transport") : anthropicTextSse("backup");
  };
  saveConfig({
    port: 0,
    defaultProvider: "first",
    providers: {
      first: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "first-key",
      },
      second: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "second-key",
      },
    },
    combos: {
      "source-stream-transport": {
        strategy: "failover",
        targets: [
          { provider: "first", model: "claude-opus-5" },
          { provider: "second", model: "claude-opus-5" },
        ],
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "combo/source-stream-transport" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify({ ...routedAnthropicSource(), stream: true }),
    });
    expect(response.status).toBe(200);
    expect(await response.text()).toContain('"text":"backup"');
    expect(sends).toBe(2);
    const entry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(entry?.attempts?.map(attempt => ({ provider: attempt.provider, sends: attempt.sendCount }))).toEqual([
      { provider: "first", sends: 1 },
      { provider: "second", sends: 1 },
    ]);
  } finally {
    await server.stop(true);
    clearRequestLogsForTests();
  }
});

test("streamed source replay fails over before output when canonical SSE is malformed", async () => {
  clearRequestLogsForTests();
  let sends = 0;
  let firstCancels = 0;
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    expect(request.url).toBe("https://api.anthropic.com/v1/messages");
    await request.body?.cancel();
    sends += 1;
    if (sends === 2) return anthropicTextSse("backup");
    const bytes = sseEncoder.encode([
      'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_bad","type":"message","role":"assistant","model":"claude-opus-5","content":[],"stop_reason":null,"usage":{"input_tokens":3,"output_tokens":0}}}\n\n',
      "data: {bad_json_marker\n\n",
      MESSAGE_STOP_FRAME,
    ].join(""));
    let pulled = false;
    return new Response(new ReadableStream<Uint8Array>({
      pull(controller) {
        if (pulled) return;
        pulled = true;
        controller.enqueue(bytes);
      },
      cancel() { firstCancels += 1; },
    }, { highWaterMark: 0 }), { headers: { "content-type": "text/event-stream" } });
  };
  saveConfig({
    port: 0,
    defaultProvider: "first",
    providers: {
      first: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "first-key",
      },
      second: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "second-key",
      },
    },
    combos: {
      "source-stream-malformed": {
        strategy: "failover",
        targets: [
          { provider: "first", model: "claude-opus-5" },
          { provider: "second", model: "claude-opus-5" },
        ],
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "combo/source-stream-malformed" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify({ ...routedAnthropicSource(), stream: true }),
    });
    expect(response.status).toBe(200);
    const body = await response.text();
    expect(body).toContain('"text":"backup"');
    expect(body).not.toContain("bad_json_marker");
    expect(sends).toBe(2);
    expect(firstCancels).toBe(1);
    const entry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(entry?.attempts?.map(attempt => ({ provider: attempt.provider, sends: attempt.sendCount }))).toEqual([
      { provider: "first", sends: 1 },
      { provider: "second", sends: 1 },
    ]);
  } finally {
    await server.stop(true);
    clearRequestLogsForTests();
  }
});

test("streamed source replay does not fail over after committed output becomes malformed", async () => {
  clearRequestLogsForTests();
  let sends = 0;
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    expect(request.url).toBe("https://api.anthropic.com/v1/messages");
    await request.body?.cancel();
    sends += 1;
    if (sends === 2) return anthropicTextSse("duplicate backup");
    const bytes = sseEncoder.encode([
      'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_partial","type":"message","role":"assistant","model":"claude-opus-5","content":[],"stop_reason":null,"usage":{"input_tokens":3,"output_tokens":0}}}\n\n',
      'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}\n\n',
      "data: {bad_json_marker\n\n",
    ].join(""));
    return new Response(bytes, { headers: { "content-type": "text/event-stream" } });
  };
  saveConfig({
    port: 0,
    defaultProvider: "first",
    providers: {
      first: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "first-key",
      },
      second: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "second-key",
      },
    },
    combos: {
      "source-stream-committed-malformed": {
        strategy: "failover",
        targets: [
          { provider: "first", model: "claude-opus-5" },
          { provider: "second", model: "claude-opus-5" },
        ],
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "combo/source-stream-committed-malformed" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify({ ...routedAnthropicSource(), stream: true }),
    });
    expect(response.status).toBe(200);
    const body = await response.text();
    expect(body).toContain('"text":"partial"');
    expect(body).toContain("anthropic passthrough protocol error");
    expect(body).not.toContain("duplicate backup");
    expect(body).not.toContain("bad_json_marker");
    expect(sends).toBe(1);
    const entry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(entry?.attempts?.map(attempt => ({ provider: attempt.provider, sends: attempt.sendCount }))).toEqual([
      { provider: "first", sends: 1 },
    ]);
  } finally {
    await server.stop(true);
    clearRequestLogsForTests();
  }
});

test("nonstream source replay fails over before output for invalid transport and JSON shapes", async () => {
  clearRequestLogsForTests();
  const malformed = new Response("{", { headers: { "content-type": "application/json" } });
  const notMessage = Response.json({ type: "not_message" });
  const incompleteMessage = Response.json({ type: "message" });
  const rootError = Response.json({
    type: "error",
    error: { type: "api_error", message: "failed" },
  });
  const wrongTransport = anthropicTextSse("wrong transport");
  const cases = [
    { model: "claude-haiku-4-5", combo: "nonstream-malformed", first: malformed },
    { model: "claude-sonnet-4-5", combo: "nonstream-shape", first: notMessage },
    { model: "claude-haiku-4-5-20251001", combo: "nonstream-envelope", first: incompleteMessage },
    { model: "claude-opus-4-6", combo: "nonstream-error", first: rootError },
    { model: "claude-sonnet-4-6", combo: "nonstream-sse", first: wrongTransport },
  ];
  const firstResponses = cases.map(testCase => testCase.first);
  let sends = 0;
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    expect(request.url).toBe("https://api.anthropic.com/v1/messages");
    expect((await request.json() as { stream?: boolean }).stream).toBe(false);
    const index = sends++;
    return index % 2 === 0
      ? firstResponses[index / 2]!
      : anthropicTextJson("backup");
  };
  saveConfig({
    port: 0,
    defaultProvider: "first",
    providers: {
      first: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "first-key",
      },
      second: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "second-key",
      },
    },
    combos: Object.fromEntries(cases.map(testCase => [testCase.combo, {
      strategy: "failover",
      targets: [
        { provider: "first", model: "claude-opus-5" },
        { provider: "second", model: "claude-opus-5" },
      ],
    }])),
    claudeCode: {
      modelMap: Object.fromEntries(cases.map(testCase => [testCase.model, `combo/${testCase.combo}`])),
    },
  } as OcxConfig);
  const server = startServer(0);
  try {
    for (const [index, testCase] of cases.entries()) {
      const response = await originalFetch(new URL("/v1/messages", server.url), {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": "placeholder" },
        body: JSON.stringify({ ...routedAnthropicSource(), model: testCase.model, stream: false }),
      });
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({ type: "message", content: [{ text: "backup" }] });
      const entry = getRequestLogEntries().findLast(row => row.surface === "claude");
      expect(entry?.attempts?.map(attempt => ({ provider: attempt.provider, sends: attempt.sendCount }))).toEqual([
        { provider: "first", sends: 1 },
        { provider: "second", sends: 1 },
      ]);
      expect(sends).toBe((index + 1) * 2);
    }
  } finally {
    await server.stop(true);
    clearRequestLogsForTests();
  }
});

test("routed canonical Anthropic preserves native server tools on source wire and bypasses media bridges", async () => {
  const captured: Record<string, unknown>[] = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    captured.push(await request.json() as Record<string, unknown>);
    return anthropicTextSse();
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
      xai: {
        adapter: "openai-chat",
        baseUrl: "https://api.x.ai/v1",
        authMode: "key",
        apiKey: "xai-test-key",
      },
    },
    images: { videoBridgeEnabled: true },
    claudeCode: { modelMap: { "claude-haiku-4-5": "target/claude-haiku-4-5" } },
  } as OcxConfig);
  const source = routedAnthropicNativeToolSource();
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(source),
    });
    expect(response.status).toBe(200);
    await response.text();
    expect(captured).toEqual([{
      ...structuredClone(source),
      model: "claude-haiku-4-5",
      stream: true,
    }]);
  } finally {
    await server.stop(true);
  }
});

test("Anthropic-native tools fail closed before incompatible upstream sends", async () => {
  const cases = [
    {
      name: "custom endpoint",
      source: routedAnthropicNativeToolSource(),
      provider: {
        adapter: "anthropic",
        baseUrl: "https://compatible.example",
        authMode: "key",
        apiKey: "selected-key",
      },
      target: "claude-opus-5",
    },
    {
      name: "cross-model native server tool",
      source: routedAnthropicNativeToolSource(),
      provider: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
      target: "claude-opus-5",
    },
    {
      name: "incompatible target model",
      source: {
        ...routedAnthropicNativeToolSource(),
        model: "claude-sonnet-4-5",
        thinking: { type: "adaptive" },
      },
      provider: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
      target: "claude-haiku-4-5",
    },
  ];

  for (const testCase of cases) {
    let sends = 0;
    globalThis.fetch = async () => {
      sends++;
      throw new Error(`unexpected ${testCase.name} upstream send`);
    };
    saveConfig({
      port: 0,
      defaultProvider: "target",
      providers: { target: testCase.provider },
      claudeCode: { modelMap: { [testCase.source.model]: `target/${testCase.target}` } },
    } as OcxConfig);
    const server = startServer(0);
    try {
      const response = await originalFetch(new URL("/v1/messages", server.url), {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": "placeholder" },
        body: JSON.stringify(testCase.source),
      });
      expect(response.status).toBe(400);
      expect(await response.text()).toMatch(/exact .*replay/);
      expect(sends).toBe(0);
    } finally {
      await server.stop(true);
    }
  }
});

test("combo rejects exact-only source without recording a physical attempt", async () => {
  clearRequestLogsForTests();
  let sends = 0;
  globalThis.fetch = async () => {
    sends++;
    throw new Error("unexpected exact-only combo upstream send");
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://compatible.example",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    combos: {
      waterfall: {
        strategy: "failover",
        targets: [{ provider: "target", model: "claude-opus-5" }],
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "combo/waterfall" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(routedAnthropicNativeToolSource()),
    });
    expect(response.status).toBe(400);
    expect(await response.text()).toContain("require exact canonical Anthropic Messages replay");
    expect(sends).toBe(0);
    const entry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(entry?.provider).toBe("combo");
    expect(entry?.attempts ?? []).toEqual([]);
  } finally {
    await server.stop(true);
    clearRequestLogsForTests();
  }
});

test("combo rejects source MCP servers before incompatible fallback", async () => {
  clearRequestLogsForTests();
  let sends = 0;
  globalThis.fetch = async () => {
    sends += 1;
    throw new Error("unexpected MCP fallback send");
  };
  saveConfig({
    port: 0,
    defaultProvider: "xai",
    providers: {
      xai: {
        adapter: "openai-chat",
        baseUrl: "https://api.x.ai/v1",
        authMode: "key",
        apiKey: "xai-test-key",
      },
    },
    combos: {
      "mcp-exact-source": {
        strategy: "failover",
        targets: [{ provider: "xai", model: "grok-4-1-fast" }],
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "combo/mcp-exact-source" } },
  } as OcxConfig);
  const source = {
    ...routedAnthropicSource(),
    mcp_servers: [{ type: "url", name: "docs", url: "https://mcp.example.test" }],
  };
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(source),
    });
    expect(response.status).toBe(400);
    expect(await response.text()).toContain("require exact canonical Anthropic Messages replay");
    expect(sends).toBe(0);
    const entry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(entry?.attempts ?? []).toEqual([]);
  } finally {
    await server.stop(true);
    clearRequestLogsForTests();
  }
});

test("combo skips source-ineligible targets before exact source replay", async () => {
  clearRequestLogsForTests();
  const captured: Record<string, unknown>[] = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    expect(request.url).toBe("https://api.anthropic.com/v1/messages");
    captured.push(await request.json() as Record<string, unknown>);
    return anthropicTextSse();
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      xai: {
        adapter: "openai-chat",
        baseUrl: "https://api.x.ai/v1",
        authMode: "key",
        apiKey: "xai-test-key",
      },
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    combos: {
      "exact-source-waterfall": {
        strategy: "failover",
        targets: [
          { provider: "xai", model: "grok-4-1-fast" },
          { provider: "target", model: "claude-opus-5" },
          { provider: "target", model: "claude-haiku-4-5" },
        ],
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "combo/exact-source-waterfall" } },
  } as OcxConfig);
  const source = routedAnthropicNativeToolSource();
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(source),
    });
    expect(response.status).toBe(200);
    await response.text();
    expect(captured).toEqual([{ ...structuredClone(source), model: "claude-haiku-4-5" }]);
    const entry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(entry?.attempts?.map(attempt => ({ provider: attempt.provider, sends: attempt.sendCount }))).toEqual([
      { provider: "target", sends: 1 },
    ]);
    expect(entry?.routeDecision?.candidates[0]).toMatchObject({
      provider: "xai",
      eligible: false,
      exclusions: [{ code: "request-incompatible" }],
    });
  } finally {
    await server.stop(true);
    clearRequestLogsForTests();
  }
});

test("combo skips incompatible canonical replay before a translated target", async () => {
  clearRequestLogsForTests();
  const captured: Array<{ url: string; body: Record<string, unknown> }> = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    captured.push({ url: request.url, body: await request.json() as Record<string, unknown> });
    return new Response(
      'data: {"choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":null}]}\n\n'
        + 'data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n',
      { headers: { "content-type": "text/event-stream" } },
    );
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
      xai: {
        adapter: "openai-chat",
        baseUrl: "https://api.x.ai/v1",
        authMode: "key",
        apiKey: "xai-test-key",
      },
    },
    combos: {
      "translated-source-waterfall": {
        strategy: "failover",
        targets: [
          { provider: "target", model: "claude-3-5-haiku-20241022" },
          { provider: "xai", model: "grok-4-1-fast" },
        ],
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "combo/translated-source-waterfall" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(routedAnthropicSource()),
    });
    expect(response.status).toBe(200);
    await response.text();
    expect(captured).toHaveLength(1);
    expect(captured[0]?.url).toBe("https://api.x.ai/v1/chat/completions");
    expect(captured[0]?.body.model).toBe("grok-4-1-fast");
    const entry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(entry?.attempts?.map(attempt => ({ provider: attempt.provider, sends: attempt.sendCount }))).toEqual([
      { provider: "xai", sends: 1 },
    ]);
  } finally {
    await server.stop(true);
    clearRequestLogsForTests();
  }
});

test("exact-only image request rejects before text-only vision sidecar dispatch", async () => {
  let sends = 0;
  globalThis.fetch = async () => {
    sends++;
    throw new Error("unexpected exact-only image upstream send");
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
        noVisionModels: ["claude-opus-5"],
      },
    },
    visionSidecar: { backend: "openai" },
    claudeCode: { modelMap: { "claude-haiku-4-5": "target/claude-opus-5" } },
  } as OcxConfig);
  const source = routedAnthropicNativeToolSource();
  source.messages = [{
    role: "user",
    content: [
      {
        type: "image",
        source: {
          type: "base64",
          media_type: "image/png",
          data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==",
        },
      },
      { type: "text", text: "inspect" },
    ],
  }];
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(source),
    });
    expect(response.status).toBe(400);
    expect(sends).toBe(0);
  } finally {
    await server.stop(true);
  }
});

test("representable web search uses translated sidecar when Anthropic source replay is incompatible", async () => {
  const exaBodies: Record<string, any>[] = [];
  const anthropicBodies: Record<string, any>[] = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url === "https://api.exa.ai/search") {
      exaBodies.push(await request.json() as Record<string, any>);
      return Response.json({
        results: [{ title: "OpenCodex", url: "https://example.test/opencodex", text: "search result" }],
      });
    }
    if (request.url !== "https://compatible.example/v1/messages") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    anthropicBodies.push(await request.json() as Record<string, any>);
    if (anthropicBodies.length === 1) {
      return new Response([
        'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_search","type":"message","role":"assistant","model":"claude-haiku-4-5","content":[],"stop_reason":null,"usage":{"input_tokens":3,"output_tokens":0}}}\n\n',
        'event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call_search","name":"web_search","input":{}}}\n\n',
        'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\\"query\\":\\"OpenCodex\\"}"}}\n\n',
        'event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n',
        'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}}\n\n',
        'event: message_stop\ndata: {"type":"message_stop"}\n\n',
      ].join(""), { headers: { "content-type": "text/event-stream" } });
    }
    return anthropicTextSse("searched");
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://compatible.example",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    webSearchSidecar: { backend: "exa", exaApiKey: "exa-test-key" },
    claudeCode: { modelMap: { "claude-sonnet-4-5": "target/claude-haiku-4-5" } },
  } as OcxConfig);
  const source = {
    model: "claude-sonnet-4-5",
    max_tokens: 128,
    stream: false,
    thinking: { type: "adaptive" },
    context_management: { source_sentinel: true },
    tools: [{ type: "web_search_20260209", name: "web_search" }],
    messages: [{ role: "user", content: "search OpenCodex" }],
  };
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(source),
    });
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("searched");
    expect(exaBodies).toHaveLength(1);
    expect(exaBodies[0]!.query).toBe("OpenCodex");
    expect(anthropicBodies).toHaveLength(2);
    expect(anthropicBodies[0]!.context_management).toBeUndefined();
    expect(anthropicBodies[0]!.tools).toContainEqual(expect.objectContaining({ name: "web_search" }));
  } finally {
    await server.stop(true);
  }
});

test("JSON Responses fallback keeps public fields and stop_sequence in synthesized Anthropic SSE", async () => {
  let sends = 0;
  let capturedBody: Record<string, unknown> | undefined;
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.openai.com/v1/responses") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    sends++;
    capturedBody = await request.json() as Record<string, unknown>;
    return Response.json({
      id: "resp_stop_sequence",
      object: "response",
      status: "completed",
      output: [{
        type: "message",
        id: "msg_stop_sequence",
        role: "assistant",
        status: "completed",
        content: [{ type: "output_text", text: "done", annotations: [] }],
      }],
      usage: { input_tokens: 3, output_tokens: 1 },
      _ocx_anthropic_stop_reason: "stop_sequence",
      _ocx_anthropic_stop_sequence: "DONE",
    });
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "openai-responses",
        baseUrl: "https://api.openai.com/v1",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "target/gpt-5.4" } },
  } as OcxConfig);
  const source = {
    model: "claude-haiku-4-5",
    max_tokens: 128,
    temperature: 0.7,
    top_p: 0.9,
    stop_sequences: ["DONE"],
    metadata: { user_id: "key-user" },
    stream: true,
    messages: [{ role: "user", content: "stop at DONE" }],
  };
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(source),
    });
    expect(response.status).toBe(200);
    const body = await response.text();
    expect(body).toContain('"stop_reason":"stop_sequence"');
    expect(body).toContain('"stop_sequence":"DONE"');
    expect(capturedBody).toMatchObject({
      max_output_tokens: 128,
      temperature: 0.7,
      top_p: 0.9,
      stop: ["DONE"],
      user: "key-user",
    });
    expect(sends).toBe(1);
  } finally {
    await server.stop(true);
  }
});

test("model-changing Opus replay accepts disabled thinking with supported effort for generation and count_tokens", async () => {
  const captured: Array<{ path: string; body: Record<string, unknown> }> = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    const path = new URL(request.url).pathname;
    const body = await request.json() as Record<string, unknown>;
    captured.push({ path, body });
    if (path === "/v1/messages/count_tokens") return Response.json({ input_tokens: 123 });
    if (path === "/v1/messages") return anthropicTextSse();
    throw new Error(`unexpected egress ${request.url}`);
  };
  const mappings = [
    ["claude-haiku-4-5", "claude-opus-4-7"],
    ["claude-sonnet-4-6", "claude-opus-4-8"],
    ["claude-opus-4-6", "claude-opus-5"],
  ] as const;
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    claudeCode: {
      modelMap: Object.fromEntries(mappings.map(([source, target]) => [source, `target/${target}`])),
    },
  } as OcxConfig);
  const server = startServer(0);
  try {
    for (const [sourceModel, targetModel] of mappings) {
      const source = {
        model: sourceModel,
        messages: [{ role: "user", content: "replay exactly" }],
        thinking: { type: "disabled" },
        output_config: { effort: "high" },
      };
      const beforeCount = getRequestLogEntries().length;
      const count = await originalFetch(new URL("/v1/messages/count_tokens", server.url), {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": "placeholder" },
        body: JSON.stringify(source),
      });
      expect(count.status).toBe(200);
      expect(await count.json()).toEqual({ input_tokens: 123 });
      expect(getRequestLogEntries()).toHaveLength(beforeCount);

      const generationBody = { ...source, max_tokens: 128, stream: true };
      const generation = await originalFetch(new URL("/v1/messages", server.url), {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": "placeholder" },
        body: JSON.stringify(generationBody),
      });
      expect(generation.status).toBe(200);
      expect(await generation.text()).toContain("message_stop");
      expect(captured.at(-2)).toEqual({ path: "/v1/messages/count_tokens", body: { ...source, model: targetModel } });
      expect(captured.at(-1)).toEqual({ path: "/v1/messages", body: { ...generationBody, model: targetModel } });
    }
  } finally {
    await server.stop(true);
  }
});

test("model-changing Opus 5 replay rejects disabled thinking with xhigh or max before dispatch", async () => {
  let sends = 0;
  globalThis.fetch = async () => {
    sends += 1;
    throw new Error("unexpected incompatible effort send");
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "target/claude-opus-5" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    for (const effort of ["xhigh", "max"]) {
      const source = {
        model: "claude-haiku-4-5",
        messages: [{ role: "user", content: "reject incompatible effort" }],
        thinking: { type: "disabled" },
        output_config: { effort },
      };
      for (const path of ["/v1/messages/count_tokens", "/v1/messages"]) {
        const response = await originalFetch(new URL(path, server.url), {
          method: "POST",
          headers: { "content-type": "application/json", "x-api-key": "placeholder" },
          body: JSON.stringify(path.endsWith("count_tokens") ? source : { ...source, max_tokens: 128, stream: true }),
        });
        expect(response.status).toBe(400);
        expect(await response.text()).toContain("replay");
      }
    }
    expect(sends).toBe(0);
  } finally {
    await server.stop(true);
  }
});

test("routed canonical source fails closed when history or thinking is incompatible with the target", async () => {
  const captured: Array<Record<string, any>> = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    captured.push(await request.json() as Record<string, any>);
    return anthropicTextSse();
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    claudeCode: {
      modelMap: {
        "claude-haiku-4-5": "target/claude-opus-5",
        "claude-sonnet-4-5": "target/claude-haiku-4-5",
        "claude-opus-4-6": "target/claude-opus-5",
        "claude-opus-5": "target/claude-opus-4-6",
      },
    },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const invalidHistory = routedAnthropicSource();
    (invalidHistory.messages as Array<Record<string, unknown>>).splice(-1, 0, {
      role: "developer",
      content: [{ type: "text", text: "unsupported mid-turn role" }],
    });
    const historyResponse = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(invalidHistory),
    });
    expect(historyResponse.status).toBe(400);
    expect(captured).toHaveLength(0);

    const adaptiveToLegacy = routedAnthropicSource();
    adaptiveToLegacy.model = "claude-sonnet-4-5";
    const legacyResponse = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(adaptiveToLegacy),
    });
    expect(legacyResponse.status).toBe(400);
    expect(captured).toHaveLength(0);

    const enabledToAdaptive = routedAnthropicSource();
    enabledToAdaptive.model = "claude-opus-4-6";
    enabledToAdaptive.thinking = { type: "enabled", budget_tokens: 4096 };
    delete enabledToAdaptive.output_config;
    const adaptiveResponse = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(enabledToAdaptive),
    });
    expect(adaptiveResponse.status).toBe(400);
    expect(captured).toHaveLength(0);

    const adaptiveTo46 = routedAnthropicSource();
    adaptiveTo46.model = "claude-opus-5";
    adaptiveTo46.mcp_servers = [{
      type: "url",
      url: "https://mcp.example.test",
      name: "source-mcp",
    }];
    const compatibleResponse = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(adaptiveTo46),
    });
    expect(compatibleResponse.status).toBe(200);
    await compatibleResponse.text();
    expect(captured).toHaveLength(1);
    expect(captured[0]).toEqual({
      ...adaptiveTo46,
      model: "claude-opus-4-6",
    });
  } finally {
    await server.stop(true);
  }
});

test("routed canonical source rejects max_tokens above the configured target cap", async () => {
  let sends = 0;
  globalThis.fetch = async () => {
    sends += 1;
    throw new Error("unexpected over-cap Anthropic send");
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
        modelMaxOutputTokens: { "claude-opus-5": 64 },
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "target/claude-opus-5" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify({ ...routedAnthropicSource(), max_tokens: 128 }),
    });
    expect(response.status).toBe(400);
    expect(await response.text()).toContain("exact replay of the caller request");
    expect(sends).toBe(0);
  } finally {
    await server.stop(true);
  }
});

test("routed canonical source rotates an expired Anthropic OAuth subscription without advancing combo", async () => {
  clearRequestLogsForTests();
  clearAnthropicAccountPoolState();
  clearPoolRotationState();
  const sends: Array<{ authorization: string | null; body: string }> = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    sends.push({ authorization: request.headers.get("authorization"), body: await request.text() });
    if (sends.length === 1) {
      return Response.json({
        type: "error",
        error: {
          type: "oauth_org_not_allowed",
          message: "Your organization has disabled Claude subscription access for Claude Code. Use an Anthropic API key instead, or ask your admin to enable access.",
        },
      }, { status: 403 });
    }
    return anthropicTextSse("rotated");
  };
  writeFileSync(join(testDir, "auth.json"), JSON.stringify({
    anthropic: {
      activeAccountId: "account-a",
      accounts: [
        {
          id: "account-a",
          credential: {
            access: "synthetic-access-a",
            refresh: "synthetic-refresh-a",
            expires: 9999999999999,
            accountId: "synthetic-account-a",
          },
        },
        {
          id: "account-b",
          credential: {
            access: "synthetic-access-b",
            refresh: "synthetic-refresh-b",
            expires: 9999999999999,
            accountId: "synthetic-account-b",
          },
        },
      ],
    },
  }), { mode: 0o600 });
  saveConfig({
    port: 0,
    defaultProvider: "anthropic",
    providers: {
      anthropic: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "oauth",
      },
    },
    anthropicAccountPool: { enabled: true, autoSwitchThreshold: 100 },
    combos: {
      waterfall: {
        strategy: "failover",
        targets: [{ provider: "anthropic", model: "claude-opus-5" }],
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "combo/waterfall" } },
  } as OcxConfig);
  const source = routedAnthropicSource();
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(source),
    });
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("rotated");
    expect(sends.map(send => send.authorization)).toEqual([
      "Bearer synthetic-access-a",
      "Bearer synthetic-access-b",
    ]);
    expect(sends[1]!.body).toBe(sends[0]!.body);
    expect(JSON.parse(sends[0]!.body)).toEqual({
      ...structuredClone(source),
      model: "claude-opus-5",
      stream: true,
    });
    const auth = JSON.parse(readFileSync(join(testDir, "auth.json"), "utf8")) as {
      anthropic: { accounts: Array<{ id: string; needsReauth?: boolean }> };
    };
    expect(auth.anthropic.accounts.find(account => account.id === "account-a")?.needsReauth).toBe(true);
    expect(auth.anthropic.accounts.find(account => account.id === "account-b")?.needsReauth).toBeUndefined();
    const entry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(entry?.provider).toBe("combo");
    expect(entry?.comboTargetAdvanced).toBeUndefined();
    expect(entry?.attempts).toHaveLength(2);
    expect(entry?.attempts?.[0]).toMatchObject({ status: 403, sendCount: 1 });
    expect(entry?.attempts?.[1]).toMatchObject({
      status: 200,
      sendCount: 1,
      recoveryKinds: ["anthropic-oauth-403"],
    });
    expect(entry?.attempts?.[1]?.provider).not.toBe(entry?.attempts?.[0]?.provider);
  } finally {
    await server.stop(true);
    clearAnthropicAccountPoolState();
    clearPoolRotationState();
    clearRequestLogsForTests();
  }
});

test("routed canonical source stops after all Anthropic OAuth accounts deny subscription access", async () => {
  clearRequestLogsForTests();
  clearAnthropicAccountPoolState();
  clearPoolRotationState();
  const denialMessage = "Your organization has disabled Claude subscription access for Claude Code. Use an Anthropic API key instead, or ask your admin to enable access.";
  const firstBody = JSON.stringify({
    type: "error",
    error: { type: "oauth_org_not_allowed", message: denialMessage, request_id: "first" },
  });
  const finalBody = JSON.stringify({
    type: "error",
    error: { type: "oauth_org_not_allowed", message: denialMessage, request_id: "final" },
  });
  let sends = 0;
  globalThis.fetch = async () => {
    sends += 1;
    return new Response(sends === 1 ? firstBody : finalBody, {
      status: 403,
      headers: { "content-type": "application/json" },
    });
  };
  writeFileSync(join(testDir, "auth.json"), JSON.stringify({
    anthropic: {
      activeAccountId: "account-a",
      accounts: [
        {
          id: "account-a",
          credential: {
            access: "synthetic-access-a",
            refresh: "synthetic-refresh-a",
            expires: 9999999999999,
            accountId: "synthetic-account-a",
          },
        },
        {
          id: "account-b",
          credential: {
            access: "synthetic-access-b",
            refresh: "synthetic-refresh-b",
            expires: 9999999999999,
            accountId: "synthetic-account-b",
          },
        },
      ],
    },
  }), { mode: 0o600 });
  saveConfig({
    port: 0,
    defaultProvider: "anthropic",
    providers: {
      anthropic: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "oauth",
      },
    },
    anthropicAccountPool: { enabled: true, autoSwitchThreshold: 100 },
    claudeCode: { modelMap: { "claude-haiku-4-5": "anthropic/claude-opus-5" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(routedAnthropicSource()),
    });
    expect(response.status).toBe(403);
    expect(await response.text()).toBe(finalBody);
    expect(sends).toBe(2);
    const auth = JSON.parse(readFileSync(join(testDir, "auth.json"), "utf8")) as {
      anthropic: { accounts: Array<{ needsReauth?: boolean }> };
    };
    expect(auth.anthropic.accounts.map(account => account.needsReauth)).toEqual([true, true]);
    const entry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(entry?.attempts).toHaveLength(2);
    expect(entry?.attempts?.map(attempt => attempt.status)).toEqual([403, 403]);
  } finally {
    await server.stop(true);
    clearAnthropicAccountPoolState();
    clearPoolRotationState();
    clearRequestLogsForTests();
  }
});

test("routed canonical source does not rotate on unrecognized Anthropic credential errors", async () => {
  clearRequestLogsForTests();
  clearAnthropicAccountPoolState();
  clearPoolRotationState();
  const denialMessage = "Your organization has disabled Claude subscription access for Claude Code. Use an Anthropic API key instead, or ask your admin to enable access.";
  const encoder = new TextEncoder();
  const cases: Array<{
    name: string;
    body: Uint8Array;
    status?: number;
    contentType?: string;
  }> = [
    {
      name: "ordinary policy denial",
      body: encoder.encode(JSON.stringify({
        type: "error",
        error: { type: "permission_error", message: denialMessage },
      })),
    },
    {
      name: "message without structured code",
      body: encoder.encode(JSON.stringify({ type: "error", error: { message: denialMessage } })),
    },
    { name: "malformed JSON", body: encoder.encode('{"type":"error","error":') },
    {
      name: "oversized JSON",
      body: encoder.encode(`${JSON.stringify({
        type: "error",
        error: { type: "oauth_org_not_allowed", message: denialMessage },
      })}${" ".repeat(65_536)}`),
    },
    {
      name: "invalid UTF-8",
      body: Uint8Array.from([
        ...encoder.encode(JSON.stringify({
          type: "error",
          error: { type: "oauth_org_not_allowed", message: denialMessage },
        })),
        0xff,
      ]),
    },
    {
      name: "HTTP 200 stream error event",
      status: 200,
      contentType: "text/event-stream",
      body: encoder.encode(`event: error\ndata: ${JSON.stringify({
        type: "error",
        error: { type: "oauth_org_not_allowed", message: denialMessage },
      })}\n\n`),
    },
  ];
  let sends = 0;
  globalThis.fetch = async () => {
    const testCase = cases[sends];
    sends += 1;
    if (!testCase) throw new Error("unexpected Anthropic retry");
    return new Response(testCase.body, {
      status: testCase.status ?? 403,
      headers: { "content-type": testCase.contentType ?? "application/json" },
    });
  };
  writeFileSync(join(testDir, "auth.json"), JSON.stringify({
    anthropic: {
      activeAccountId: "account-a",
      accounts: [
        {
          id: "account-a",
          credential: {
            access: "synthetic-access-a",
            refresh: "synthetic-refresh-a",
            expires: 9999999999999,
            accountId: "synthetic-account-a",
          },
        },
        {
          id: "account-b",
          credential: {
            access: "synthetic-access-b",
            refresh: "synthetic-refresh-b",
            expires: 9999999999999,
            accountId: "synthetic-account-b",
          },
        },
      ],
    },
  }), { mode: 0o600 });
  saveConfig({
    port: 0,
    defaultProvider: "anthropic",
    providers: {
      anthropic: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "oauth",
      },
    },
    anthropicAccountPool: { enabled: true, autoSwitchThreshold: 100 },
    claudeCode: { modelMap: { "claude-haiku-4-5": "anthropic/claude-opus-5" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    for (const testCase of cases) {
      const response = await originalFetch(new URL("/v1/messages", server.url), {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": "placeholder" },
        body: JSON.stringify(routedAnthropicSource()),
      });
      expect(response.status, testCase.name).toBe(testCase.status ?? 403);
      expect(new Uint8Array(await response.arrayBuffer()), testCase.name).toEqual(testCase.body);
    }
    expect(sends).toBe(cases.length);
    const auth = JSON.parse(readFileSync(join(testDir, "auth.json"), "utf8")) as {
      anthropic: { accounts: Array<{ needsReauth?: boolean }> };
    };
    expect(auth.anthropic.accounts.map(account => account.needsReauth)).toEqual([undefined, undefined]);
    const entries = getRequestLogEntries().filter(row => row.surface === "claude");
    expect(entries).toHaveLength(cases.length);
    expect(entries.every(entry => entry.attempts?.length === 1)).toBe(true);
  } finally {
    await server.stop(true);
    clearAnthropicAccountPoolState();
    clearPoolRotationState();
    clearRequestLogsForTests();
  }
});

test("routed canonical source preserves combo attempts across Anthropic OAuth account rotation", async () => {
  clearRequestLogsForTests();
  clearAnthropicAccountPoolState();
  clearPoolRotationState();
  const sends: Array<{ authorization: string | null; body: string }> = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    sends.push({ authorization: request.headers.get("authorization"), body: await request.text() });
    if (sends.length === 1) {
      return Response.json(
        { type: "error", error: { type: "rate_limit_error", message: "rate limited" } },
        { status: 429, headers: { "retry-after": "30" } },
      );
    }
    return anthropicTextSse("rotated");
  };
  writeFileSync(join(testDir, "auth.json"), JSON.stringify({
    anthropic: {
      activeAccountId: "account-a",
      accounts: [
        {
          id: "account-a",
          credential: {
            access: "synthetic-access-a",
            refresh: "synthetic-refresh-a",
            expires: 9999999999999,
            accountId: "synthetic-account-a",
          },
        },
        {
          id: "account-b",
          credential: {
            access: "synthetic-access-b",
            refresh: "synthetic-refresh-b",
            expires: 9999999999999,
            accountId: "synthetic-account-b",
          },
        },
      ],
    },
  }), { mode: 0o600 });
  saveConfig({
    port: 0,
    defaultProvider: "anthropic",
    providers: {
      anthropic: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "oauth",
      },
    },
    anthropicAccountPool: { enabled: true, autoSwitchThreshold: 100 },
    combos: {
      waterfall: {
        strategy: "failover",
        targets: [{ provider: "anthropic", model: "claude-opus-5" }],
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "combo/waterfall" } },
  } as OcxConfig);
  const source = routedAnthropicSource();
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(source),
    });
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("rotated");
    expect(sends.map(send => send.authorization)).toEqual([
      "Bearer synthetic-access-a",
      "Bearer synthetic-access-b",
    ]);
    expect(sends).toHaveLength(2);
    expect(sends[1]!.body).toBe(sends[0]!.body);
    expect(JSON.parse(sends[0]!.body)).toEqual({
      ...structuredClone(source),
      model: "claude-opus-5",
      stream: true,
    });
    const entry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(entry?.provider).toBe("combo");
    expect(entry?.comboTargetAdvanced).toBeUndefined();
    expect(entry?.attempts).toHaveLength(2);
    expect(entry?.attempts?.[0]).toMatchObject({
      status: 429,
      sendCount: 1,
    });
    expect(entry?.attempts?.[1]).toMatchObject({
      status: 200,
      sendCount: 1,
      recoveryKinds: ["anthropic-oauth-429"],
    });
    expect(entry?.attempts?.[1]?.provider).not.toBe(entry?.attempts?.[0]?.provider);
  } finally {
    await server.stop(true);
    clearAnthropicAccountPoolState();
    clearPoolRotationState();
    clearRequestLogsForTests();
  }
});

test("routed canonical source rebuilds only images after Anthropic 413", async () => {
  clearRequestLogsForTests();
  const onePxPng = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==";
  const png = await new Bun.Image(Buffer.from(onePxPng, "base64"))
    .resize(1500, 1000)
    .png()
    .toBuffer();
  const source = {
    model: "claude-haiku-4-5",
    max_tokens: 128,
    stream: false,
    messages: [{
      role: "user",
      content: [
        {
          type: "image",
          source: {
            type: "base64",
            media_type: "image/png",
            data: Buffer.from(png).toString("base64"),
          },
          cache_control: { type: "ephemeral", ttl: "1h" },
        },
        { type: "text", text: "look" },
      ],
    }],
    context_management: { source_sentinel: true },
  };
  const snapshot = structuredClone(source);
  const sends: Array<{ apiKey: string | null; body: Record<string, any> }> = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    sends.push({
      apiKey: request.headers.get("x-api-key"),
      body: await request.json() as Record<string, any>,
    });
    if (sends.length === 1) {
      return Response.json(
        { type: "error", error: { type: "request_too_large", message: "too large" } },
        { status: 413 },
      );
    }
    return anthropicTextJson("resized");
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "target/claude-opus-5" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(source),
    });
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("resized");
    expect(source).toEqual(snapshot);
    expect(sends).toHaveLength(2);
    expect(sends.map(send => send.apiKey)).toEqual(["selected-key", "selected-key"]);
    expect(sends[0]!.body).toEqual({ ...structuredClone(snapshot), model: "claude-opus-5" });
    const firstImage = sends[0]!.body.messages[0].content[0];
    const secondImage = sends[1]!.body.messages[0].content[0];
    expect(firstImage.source.media_type).toBe("image/png");
    expect(secondImage.source.media_type).toBe("image/jpeg");
    expect(firstImage.cache_control).toEqual({ type: "ephemeral", ttl: "1h" });
    expect(secondImage.cache_control).toEqual(firstImage.cache_control);
    const expectedRetry = structuredClone(sends[0]!.body);
    expectedRetry.messages[0].content[0] = secondImage;
    expect(sends[1]!.body).toEqual(expectedRetry);
    const entry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(entry?.attempts).toHaveLength(1);
    expect(entry?.attempts?.[0]).toMatchObject({ sendCount: 2, recoveryKinds: ["image-413"] });
  } finally {
    await server.stop(true);
    clearRequestLogsForTests();
  }
});

test("routed canonical source relays successful empty and max-token terminals without hidden sends", async () => {
  clearRequestLogsForTests();
  const sends: string[] = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    sends.push(await request.text());
    if (sends.length === 1) {
      return new Response([
        'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_empty","type":"message","role":"assistant","model":"claude-opus-5","content":[],"stop_reason":null,"usage":{"input_tokens":3,"output_tokens":0}}}\n\n',
        'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":0}}\n\n',
        'event: message_stop\ndata: {"type":"message_stop"}\n\n',
      ].join(""), { headers: { "content-type": "text/event-stream" } });
    }
    if (sends.length === 2) return anthropicTextSse("limited", "max_tokens");
    throw new Error("unexpected hidden send");
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    emptyCompletionRetry: true,
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    claudeCode: {
      modelMap: {
        "claude-haiku-4-5": "target/claude-opus-5",
        "claude-sonnet-4-5": "target/claude-opus-5",
      },
    },
  } as OcxConfig);
  const server = startServer(0);
  try {
    const source = routedAnthropicSource();
    const retried = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(source),
    });
    expect(retried.status).toBe(200);
    expect(await retried.text()).toContain("message_stop");
    expect(sends).toHaveLength(1);
    const firstEntry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(firstEntry?.attempts?.[0]).toMatchObject({ sendCount: 1 });
    expect(firstEntry?.attempts?.[0]?.recoveryKinds).toEqual([]);

    source.model = "claude-sonnet-4-5";
    const limited = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(source),
    });
    expect(limited.status).toBe(200);
    expect(await limited.text()).toContain("limited");
    expect(sends).toHaveLength(2);
    expect(JSON.parse(sends[1]!)).toEqual({
      ...structuredClone(source),
      model: "claude-opus-5",
      stream: true,
    });
    const lastEntry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(lastEntry?.attempts?.[0]).toMatchObject({ sendCount: 1 });
    expect(lastEntry?.attempts?.[0]?.recoveryKinds).toEqual([]);
  } finally {
    await server.stop(true);
    clearRequestLogsForTests();
  }
});

test("routed canonical source settles protocol terminals without waiting for transport EOF", async () => {
  clearRequestLogsForTests();
  const completed = sseEncoder.encode('event: message_stop\ndata: {"type":"message_stop"}\n\n');
  const failed = sseEncoder.encode('event: error\ndata: {"type":"error","error":{"type":"overloaded_error","message":"busy"}}\n\n');
  const cases = [
    { name: "open success", frame: completed, delayedFailure: false, status: 200, terminalStatus: "completed" },
    { name: "late failure after success", frame: completed, delayedFailure: true, status: 200, terminalStatus: "completed" },
    { name: "open error", frame: failed, delayedFailure: false, status: 502, terminalStatus: "failed" },
    { name: "late failure after error", frame: failed, delayedFailure: true, status: 502, terminalStatus: "failed" },
  ] as const;
  let current = cases[0]!;
  let cancels = 0;

  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    await request.text();
    let pulls = 0;
    return new Response(new ReadableStream<Uint8Array>({
      pull(controller) {
        pulls += 1;
        if (pulls === 1) {
          controller.enqueue(current.frame);
          return;
        }
        if (current.delayedFailure) {
          return new Promise<void>((_resolve, reject) => {
            setTimeout(() => reject(new Error("late transport failure")), 25);
          });
        }
        return new Promise<void>(() => {});
      },
      cancel() { cancels += 1; },
    }), { headers: { "content-type": "text/event-stream" } });
  };
  saveConfig({
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "target/claude-opus-5" } },
  } as OcxConfig);
  const server = startServer(0);
  try {
    for (const testCase of cases) {
      current = testCase;
      clearRequestLogsForTests();
      const response = await originalFetch(new URL("/v1/messages", server.url), {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": "placeholder" },
        body: JSON.stringify(routedAnthropicSource()),
      });
      expect(response.status, testCase.name).toBe(200);
      const bytes = new Uint8Array(await response.arrayBuffer());
      expect(bytes, testCase.name).toEqual(testCase.frame);
      expect(new TextDecoder().decode(bytes), testCase.name).not.toContain("adapter_eof");
      const rows = getRequestLogEntries().filter(row => row.surface === "claude");
      expect(rows, testCase.name).toHaveLength(1);
      expect(rows[0], testCase.name).toMatchObject({
        status: testCase.status,
        terminalStatus: testCase.terminalStatus,
        closeReason: "terminal",
        attempts: [{ provider: "target", model: "claude-opus-5", sendCount: 1 }],
      });
    }
    expect(cancels).toBe(cases.length);
  } finally {
    await server.stop(true);
    clearRequestLogsForTests();
  }
});

test("same metadata user keeps concurrent routed source aborts isolated", async () => {
  clearRequestLogsForTests();
  const encoder = new TextEncoder();
  type PhysicalRequest = {
    signal: AbortSignal;
    controller: ReadableStreamDefaultController<Uint8Array>;
    body: Record<string, any>;
  };
  const physical: PhysicalRequest[] = [];
  let resolveFirstSend!: () => void;
  let resolveSecondSend!: () => void;
  let resolveFirstAbort!: () => void;
  const firstSend = new Promise<void>(resolve => { resolveFirstSend = resolve; });
  const secondSend = new Promise<void>(resolve => { resolveSecondSend = resolve; });
  const firstAbort = new Promise<void>(resolve => { resolveFirstAbort = resolve; });

  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url !== "https://api.anthropic.com/v1/messages") {
      throw new Error(`unexpected egress ${request.url}`);
    }
    let controller!: ReadableStreamDefaultController<Uint8Array>;
    const stream = new ReadableStream<Uint8Array>({
      start(next) {
        controller = next;
        next.enqueue(encoder.encode([
          'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_held","type":"message","role":"assistant","model":"claude-opus-5","content":[],"stop_reason":null,"usage":{"input_tokens":3,"output_tokens":0}}}\n\n',
          'event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n',
          'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"held"}}\n\n',
        ].join("")));
      },
    });
    const entry = {
      signal: request.signal,
      controller,
      body: await request.json() as Record<string, any>,
    };
    physical.push(entry);
    request.signal.addEventListener("abort", () => {
      try { controller.error(request.signal.reason); } catch { /* already settled */ }
      if (physical[0] === entry) resolveFirstAbort();
    }, { once: true });
    if (physical.length === 1) resolveFirstSend();
    if (physical.length === 2) resolveSecondSend();
    return new Response(stream, { headers: { "content-type": "text/event-stream" } });
  };

  const config = {
    port: 0,
    defaultProvider: "target",
    providers: {
      target: {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-key",
      },
    },
    combos: {
      isolated: {
        strategy: "failover",
        targets: [{ provider: "target", model: "claude-opus-5" }],
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "combo/isolated" } },
  } as OcxConfig;
  const invoke = (client: AbortController, text: string) => handleClaudeMessages(
    new Request("http://localhost/v1/messages", {
      method: "POST",
      signal: client.signal,
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify({
        model: "claude-haiku-4-5",
        max_tokens: 16,
        stream: true,
        metadata: { user_id: "shared-user-id" },
        messages: [{ role: "user", content: text }],
      }),
    }),
    config,
    { model: "", provider: "" } as RequestLogContext,
  );

  try {
    const firstClient = new AbortController();
    const firstPending = invoke(firstClient, "first");
    await firstSend;
    const secondClient = new AbortController();
    const secondPending = invoke(secondClient, "second");
    await secondSend;
    const [firstResponse, secondResponse] = await Promise.all([firstPending, secondPending]);
    expect(firstResponse.status).toBe(200);
    expect(secondResponse.status).toBe(200);
    expect(physical).toHaveLength(2);
    expect(physical[0]!.body.metadata).toEqual(physical[1]!.body.metadata);

    firstClient.abort(new DOMException("first client closed", "AbortError"));
    await firstAbort;
    expect(physical[0]!.signal.aborted).toBe(true);
    expect(physical[1]!.signal.aborted).toBe(false);

    physical[1]!.controller.enqueue(encoder.encode([
      'event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n',
      'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}\n\n',
      'event: message_stop\ndata: {"type":"message_stop"}\n\n',
    ].join("")));
    physical[1]!.controller.close();
    expect(await secondResponse.text()).toContain("message_stop");
  } finally {
    for (const entry of physical) {
      if (!entry.signal.aborted) {
        try { entry.controller.close(); } catch { /* already settled */ }
      }
    }
    clearRequestLogsForTests();
  }
});

test("routed source replay is restricted to the exact canonical Anthropic Messages URL", async () => {
  const destinations = [
    "http://api.anthropic.com",
    "https://api.anthropic.com.evil.test",
    "https://api.anthropic.com:8443",
    "https://api.anthropic.com/proxy",
    "https://compatible.example/v1",
  ];

  for (const baseUrl of destinations) {
    const captured: Array<{
      url: string;
      headers: Headers;
      body: Record<string, unknown>;
      keepalive?: boolean;
    }> = [];
    globalThis.fetch = async (input, init) => {
      const request = new Request(input, init);
      captured.push({
        url: request.url,
        headers: request.headers,
        body: await request.json() as Record<string, unknown>,
        keepalive: init?.keepalive,
      });
      return anthropicTextSse();
    };
    saveConfig({
      port: 0,
      defaultProvider: "target",
      providers: {
        target: { adapter: "anthropic", baseUrl, authMode: "key", apiKey: "selected-key" },
      },
      claudeCode: {
        modelMap: { "claude-haiku-4-5": "target/claude-opus-5" },
      },
    } as OcxConfig);
    const server = startServer(0);
    try {
      const response = await originalFetch(new URL("/v1/messages", server.url), {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": "caller-key" },
        body: JSON.stringify(routedAnthropicSource()),
      });
      expect(response.status).toBe(200);
      await response.text();
      expect(captured).toHaveLength(1);
      expect(captured[0]!.body.context_management).toBeUndefined();
      expect(captured[0]!.headers.get("x-api-key")).toBe("selected-key");
      expect(captured[0]!.headers.get("authorization")).toBeNull();
      expect(captured[0]!.headers.get("connection")).toBeNull();
      expect(captured[0]!.keepalive).toBeUndefined();
    } finally {
      await server.stop(true);
    }
  }

  const parsed = {
    modelId: "claude-opus-5",
    stream: true,
    options: {},
    context: { systemPrompt: ["system"], messages: [{ role: "user", content: "hello" }] },
  } as OcxParsedRequest;
  for (const baseUrl of [
    "https://api.anthropic.com?target=proxy",
    "https://api.anthropic.com#proxy",
  ]) {
    const request = await createAnthropicAdapter({
      adapter: "anthropic",
      baseUrl,
      authMode: "key",
      apiKey: "selected-key",
    }).buildRequest(parsed, {
      headers: new Headers(),
      translatorBudget: createTestTranslatorBudget(),
      anthropicMessagesSource: { body: routedAnthropicSource(), headers: {} },
    });
    expect(request.anthropicSourceReplay).toBeUndefined();
    expect((JSON.parse(request.body) as Record<string, unknown>).context_management).toBeUndefined();
  }
});

test("mixed combo isolates source wire and never fails over after visible output", async () => {
  clearRequestLogsForTests();
  const { server: backup, captured: backupBodies } = mockChatUpstreamCapturing();
  const anthropicBodies: Array<Record<string, unknown>> = [];
  let failAfterVisibleOutput = false;
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url === "https://api.anthropic.com/v1/messages") {
      anthropicBodies.push(await request.json() as Record<string, unknown>);
      if (failAfterVisibleOutput) {
        return new Response([
          'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_test","type":"message","role":"assistant","model":"claude-opus-5","content":[],"stop_reason":null,"usage":{"input_tokens":3,"output_tokens":0}}}\n\n',
          'event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n',
          'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"already visible"}}\n\n',
          'event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n',
          'event: error\ndata: {"type":"error","error":{"type":"overloaded_error","message":"late failure"}}\n\n',
        ].join(""), { headers: { "content-type": "text/event-stream" } });
      }
      return new Response(
        'event: error\ndata: {"type":"error","error":{"type":"overloaded_error","message":"busy"}}\n\n',
        { headers: { "content-type": "text/event-stream" } },
      );
    }
    return originalFetch(input, init);
  };
  saveConfig({
    port: 0,
    defaultProvider: "source-anthropic",
    providers: {
      "source-anthropic": {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-anthropic-key",
      },
      "source-anthropic-visible": {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-anthropic-key",
      },
      backup: {
        adapter: "openai-chat",
        baseUrl: `${backup.url.toString().replace(/\/$/, "")}/v1`,
        authMode: "key",
        apiKey: "backup-key",
        allowPrivateNetwork: true,
      },
    },
    combos: {
      waterfall: {
        strategy: "failover",
        targets: [
          { provider: "source-anthropic", model: "claude-opus-5" },
          { provider: "backup", model: "gpt-test" },
        ],
      },
      "visible-waterfall": {
        strategy: "failover",
        targets: [
          { provider: "source-anthropic-visible", model: "claude-opus-5" },
          { provider: "backup", model: "gpt-test" },
        ],
      },
    },
    claudeCode: {
      modelMap: {
        "claude-haiku-4-5": "combo/waterfall",
        "claude-sonnet-4-5": "combo/visible-waterfall",
      },
    },
  } as OcxConfig);
  const source = routedAnthropicSource();
  const server = startServer(0);
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(source),
    });
    expect(response.status).toBe(200);
    const failoverBody = await response.text();
    expect(failoverBody).toContain('"text":"Hello"');
    expect(failoverBody).toContain('"text":" from mock"');
    expect(anthropicBodies).toEqual([{ ...structuredClone(source), model: "claude-opus-5", stream: true }]);
    expect(backupBodies).toHaveLength(1);
    expect(backupBodies[0]!.context_management).toBeUndefined();
    const entry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(entry?.attempts).toMatchObject([
      { provider: "source-anthropic", model: "claude-opus-5", sendCount: 1 },
      { provider: "backup", model: "gpt-test", sendCount: 1 },
    ]);

    failAfterVisibleOutput = true;
    source.model = "claude-sonnet-4-5";
    source.stream = true;
    const lateFailure = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(source),
    });
    expect(lateFailure.status).toBe(200);
    const lateFailureBody = await lateFailure.text();
    expect(lateFailureBody).toContain("already visible");
    expect(lateFailureBody).not.toContain("Hello from mock");
    expect(anthropicBodies).toHaveLength(2);
    expect(anthropicBodies[1]).toEqual({ ...structuredClone(source), model: "claude-opus-5", stream: true });
    expect(backupBodies).toHaveLength(1);
    const lateEntry = getRequestLogEntries().findLast(row => row.surface === "claude");
    expect(lateEntry?.attempts).toHaveLength(1);
    expect(lateEntry?.attempts).toMatchObject([
      { provider: "source-anthropic-visible", model: "claude-opus-5", sendCount: 1 },
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    await server.stop(true);
    backup.stop(true);
    clearRequestLogsForTests();
  }
});

test("speculative raw Anthropic failure keeps parent admission through accepted backup body", async () => {
  clearRequestLogsForTests();
  const encoder = new TextEncoder();
  let finishBackup!: () => void;
  let backupFinished = false;
  const backup = Bun.serve({
    port: 0,
    fetch() {
      return new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(encoder.encode(
            `data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: "held backup" }, finish_reason: null }] })}\n\n`,
          ));
          finishBackup = () => {
            if (backupFinished) return;
            backupFinished = true;
            controller.enqueue(encoder.encode([
              `data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 2, completion_tokens: 1 } })}\n\n`,
              "data: [DONE]\n\n",
            ].join("")));
            controller.close();
          };
        },
      }), { headers: { "content-type": "text/event-stream" } });
    },
  });
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    if (request.url === "https://api.anthropic.com/v1/messages") {
      await request.text();
      return new Response(
        'event: error\ndata: {"type":"error","error":{"type":"overloaded_error","message":"busy"}}\n\n',
        { headers: { "content-type": "text/event-stream" } },
      );
    }
    return originalFetch(input, init);
  };
  saveConfig({
    port: 0,
    defaultProvider: "source-anthropic",
    providers: {
      "source-anthropic": {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "selected-anthropic-key",
      },
      backup: {
        adapter: "openai-chat",
        baseUrl: `${backup.url.toString().replace(/\/$/, "")}/v1`,
        authMode: "key",
        apiKey: "backup-key",
        allowPrivateNetwork: true,
      },
    },
    combos: {
      "admission-waterfall": {
        strategy: "failover",
        targets: [
          { provider: "source-anthropic", model: "claude-opus-5" },
          { provider: "backup", model: "gpt-test" },
        ],
      },
    },
    claudeCode: { modelMap: { "claude-haiku-4-5": "combo/admission-waterfall" } },
  } as OcxConfig);
  const server = startServer(0);
  const before = activeRegistryMetrics().activeTurns.active;
  try {
    const response = await originalFetch(new URL("/v1/messages", server.url), {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "placeholder" },
      body: JSON.stringify(routedAnthropicSource()),
    });
    expect(response.status).toBe(200);
    expect(activeRegistryMetrics().activeTurns.active).toBe(before + 1);
    finishBackup();
    expect(await response.text()).toContain("held backup");
    expect(activeRegistryMetrics().activeTurns.active).toBe(before);
    expect(getRequestLogEntries().findLast(row => row.surface === "claude")?.attempts).toMatchObject([
      { provider: "source-anthropic", status: 529, sendCount: 1 },
      { provider: "backup", status: 200, sendCount: 1 },
    ]);
  } finally {
    finishBackup?.();
    await server.stop(true);
    backup.stop(true);
    clearRequestLogsForTests();
  }
});

test("generated agent effort directive preserves routed Anthropic structured output", async () => {
  const captured: Array<Record<string, unknown>> = [];
  const upstream = Bun.serve({
    port: 0,
    async fetch(req) {
      captured.push(await req.json() as Record<string, unknown>);
      return new Response([
        'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_test","type":"message","role":"assistant","model":"claude-sonnet-5","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":0}}}\n\n',
        'event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n',
        'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"{\\"answer\\":\\"ok\\"}"}}\n\n',
        'event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n',
        'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}\n\n',
        'event: message_stop\ndata: {"type":"message_stop"}\n\n',
      ].join(""), { headers: { "content-type": "text/event-stream" } });
    },
  });
  saveConfig({
    port: 0,
    defaultProvider: "mock-anthropic",
    providers: {
      "mock-anthropic": {
        adapter: "anthropic",
        baseUrl: upstream.url.toString().replace(/\/$/, ""),
        apiKey: "test-key",
        allowPrivateNetwork: true,
      },
    },
  } as OcxConfig);
  const server = startServer(0);
  const schema = {
    type: "object",
    properties: { answer: { type: "string" } },
    required: ["answer"],
    additionalProperties: false,
  };
  try {
    const response = await postMessages(server.url.toString(), {
      model: "claude-haiku-4-5",
      max_tokens: 32000,
      stream: true,
      system: [
        { type: "text", text: "<!-- ocx-route: claude-ocx-mock-anthropic--claude-sonnet-5 -->" },
        { type: "text", text: "<!-- ocx-effort: max -->" },
      ],
      thinking: { type: "enabled", budget_tokens: 31999 },
      output_config: {
        format: { type: "json_schema", schema },
      },
      messages: [{ role: "user", content: "Return JSON" }],
    });
    expect(response.status).toBe(200);
    await response.text();
    expect(captured).toHaveLength(1);
    expect(captured[0]!.output_config).toEqual({
      effort: "max",
      format: { type: "json_schema", schema },
    });
  } finally {
    await server.stop(true);
    upstream.stop(true);
  }
});

test("unknown-ladder routes keep the requested effort (no false stripping)", async () => {
  const { server: upstream, captured } = mockChatUpstreamCapturing();
  saveConfig(mockConfig(`${upstream.url.toString().replace(/\/$/, "")}/v1`));
  const server = startServer(0);
  try {
    const response = await postMessages(server.url.toString(), {
      model: "mock/test-model",
      max_tokens: 64,
      stream: true,
      thinking: { type: "adaptive" },
      output_config: { effort: "low" },
      messages: [{ role: "user", content: "hi" }],
    });
    expect(response.status).toBe(200);
    await response.text();
    expect(captured.length).toBe(1);
    expect(captured[0]!.reasoning_effort).toBe("low");
  } finally {
    await server.stop(true);
    upstream.stop(true);
  }
});

test("defensive [1m] strip: a leaked context-variant marker still routes to the bare model (devlog 138)", async () => {
  const { server: upstream, captured } = mockChatUpstreamCapturing();
  saveConfig(mockConfig(`${upstream.url.toString().replace(/\/$/, "")}/v1`));
  const server = startServer(0);
  try {
    const response = await postMessages(server.url.toString(), {
      model: "mock/test-model[1m]",
      max_tokens: 64,
      stream: true,
      messages: [{ role: "user", content: "hi" }],
    });
    expect(response.status).toBe(200);
    await response.text();
    expect(captured.length).toBe(1);
    expect(captured[0]!.model).toBe("test-model");
  } finally {
    await server.stop(true);
    upstream.stop(true);
  }
});

test("count_tokens is CJK-aware: Korean body counts more tokens than equal-length English (devlog 260712 B3)", async () => {
  saveConfig(mockConfig("http://127.0.0.1:1/v1"));
  const server = startServer(0);
  try {
    const count = async (content: string) => {
      const res = await fetch(new URL("/v1/messages/count_tokens", server.url), {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": "placeholder" },
        body: JSON.stringify({ model: "mock/test-model", messages: [{ role: "user", content }] }),
      });
      return (await res.json() as { input_tokens: number }).input_tokens;
    };
    const korean = "가나다라마바사아자차카타파하".repeat(40);
    const english = "abcdefghijklmn".repeat(40); // same char length
    expect(korean.length).toBe(english.length);
    expect(await count(korean)).toBeGreaterThan(await count(english));
  } finally {
    await server.stop(true);
  }
});
