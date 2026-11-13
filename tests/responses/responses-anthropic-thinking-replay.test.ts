import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { resetWorkflowBudgetsForTest } from "../../src/lib/workflow-budget";
import { encodeReasoningEnvelope } from "../../src/responses/reasoning-envelope";
import { handleResponses } from "../../src/server/responses/core";
import type { RequestLogContext } from "../../src/server/request-log";
import type { OcxConfig } from "../../src/types";
import { acquireOwnedSpendHome } from "../helpers/owned-spend-home";

const originalFetch = globalThis.fetch;
const originalHome = process.env.OPENCODEX_HOME;
const BAD_SIGNATURE = { type: "error", error: { type: "invalid_request_error", message: "messages.3.content.0: Invalid `signature` in `thinking` block" } };
const OTHER_400 = { type: "error", error: { type: "invalid_request_error", message: "max_tokens: must be positive" } };
const SIG = "EqQBCkYIARgCKkD".padEnd(48, "x");
let testDir = "";
let releaseSpendHome: (() => void) | undefined;

function config(): OcxConfig {
  return {
    defaultProvider: "anthropic-apikey",
    providers: {
      "anthropic-apikey": {
        adapter: "anthropic",
        baseUrl: "https://api.anthropic.com",
        authMode: "key",
        apiKey: "test-token",
        models: ["claude-opus-5-5"],
      },
    },
  } as OcxConfig;
}

const reasoning = { type: "reasoning", id: "rs_1", summary: [], encrypted_content: encodeReasoningEnvelope({ sig: SIG, txt: "thought" }) };
const user = { type: "message", role: "user", content: [{ type: "input_text", text: "hi" }] };
const toolLoop = [
  reasoning,
  { type: "function_call", call_id: "call_1", name: "lookup", arguments: "{}" },
  { type: "function_call_output", call_id: "call_1", output: "result" },
];
const closedTurn = [
  reasoning,
  { type: "message", role: "assistant", content: [{ type: "output_text", text: "done" }] },
  { type: "message", role: "user", content: [{ type: "input_text", text: "again" }] },
];

function request(history: unknown[]): Request {
  return new Request("http://localhost/v1/responses", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      model: "anthropic-apikey/claude-opus-5-5",
      stream: false,
      store: false,
      reasoning: { effort: "high" },
      tools: [{ type: "function", name: "lookup", parameters: { type: "object", properties: {} } }],
      input: [user, ...history],
    }),
  });
}

function success(): Response {
  return Response.json({
    id: "msg-ok", type: "message", role: "assistant", model: "claude-opus-5-5",
    content: [{ type: "text", text: "ok" }], stop_reason: "end_turn",
    usage: { input_tokens: 1, output_tokens: 1 },
  });
}

function fakeUpstream(answer: (index: number) => Response): Array<Record<string, any>> {
  const sends: Array<Record<string, any>> = [];
  globalThis.fetch = (async (_input: RequestInfo | URL, init?: RequestInit) => {
    sends.push(JSON.parse(String(init?.body)) as Record<string, any>);
    return answer(sends.length);
  }) as typeof fetch;
  return sends;
}

const thinkingBlocks = (send: Record<string, any>): unknown[] =>
  send.messages.flatMap((m: any) => (Array.isArray(m.content) ? m.content : []))
    .filter((b: any) => b.type === "thinking" || b.type === "redacted_thinking");

beforeEach(() => {
  resetWorkflowBudgetsForTest();
  testDir = mkdtempSync(join(tmpdir(), "ocx-anthropic-thinking-replay-"));
  process.env.OPENCODEX_HOME = testDir;
  releaseSpendHome = acquireOwnedSpendHome();
});

afterEach(() => {
  try {
    releaseSpendHome?.();
  } finally {
    releaseSpendHome = undefined;
    globalThis.fetch = originalFetch;
    if (originalHome === undefined) delete process.env.OPENCODEX_HOME;
    else process.env.OPENCODEX_HOME = originalHome;
    rmSync(testDir, { recursive: true, force: true });
    resetWorkflowBudgetsForTest();
  }
});

describe("Anthropic thinking-signature rejection in Responses dispatch", () => {
  test("a closed turn is resent once without history thinking blocks, thinking stays on", async () => {
    const sends = fakeUpstream(index => index === 1 ? Response.json(BAD_SIGNATURE, { status: 400 }) : success());
    const logCtx: RequestLogContext = { model: "", provider: "" };
    const response = await handleResponses(request(closedTurn), config(), logCtx);
    await response.text();

    expect(response.status).toBe(200);
    expect(sends).toHaveLength(2);
    expect(thinkingBlocks(sends[0]!)).toHaveLength(1);
    expect(thinkingBlocks(sends[1]!)).toHaveLength(0);
    expect(sends[1]!.thinking?.type).toBe(sends[0]!.thinking?.type);
    expect(sends[1]!.thinking?.type).not.toBe("disabled");
    expect(logCtx.activeAttempt?.recoveryKinds).toEqual(["anthropic-thinking-replay"]);
  });

  test("inside an open tool loop the resend turns thinking off", async () => {
    const sends = fakeUpstream(index => index === 1 ? Response.json(BAD_SIGNATURE, { status: 400 }) : success());
    const response = await handleResponses(request(toolLoop), config(), { model: "", provider: "" });
    await response.text();

    expect(response.status).toBe(200);
    expect(sends).toHaveLength(2);
    expect(sends[0]!.thinking).toBeDefined();
    expect(thinkingBlocks(sends[1]!)).toHaveLength(0);
    expect(sends[1]!.messages.some((m: any) => Array.isArray(m.content) && m.content.some((b: any) => b.type === "tool_use"))).toBe(true);
    expect([undefined, "disabled"]).toContain(sends[1]!.thinking?.type);
  });

  test("an unrelated 400 is not resent", async () => {
    const sends = fakeUpstream(() => Response.json(OTHER_400, { status: 400 }));
    const logCtx: RequestLogContext = { model: "", provider: "" };
    const response = await handleResponses(request(closedTurn), config(), logCtx);
    await response.text();

    expect(response.status).toBe(400);
    expect(sends).toHaveLength(1);
    expect(logCtx.activeAttempt?.recoveryKinds ?? []).not.toContain("anthropic-thinking-replay");
  });

  test("a second signature rejection is returned after one resend", async () => {
    const sends = fakeUpstream(() => Response.json(BAD_SIGNATURE, { status: 400 }));
    const response = await handleResponses(request(closedTurn), config(), { model: "", provider: "" });
    const body = await response.text();

    expect(response.status).toBe(400);
    expect(body).toContain("Invalid `signature`");
    expect(sends).toHaveLength(2);
  });
});
