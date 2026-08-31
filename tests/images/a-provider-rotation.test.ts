import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { clearKeyCooldowns } from "../../src/providers/key-failover";
import { resetProviderRequestPacingForTest } from "../../src/providers/request-pacing";
import { handleResponses } from "../../src/server/responses";
import type { OcxConfig, OcxProviderConfig } from "../../src/types";

let previousHome: string | undefined;
let testHome = "";

beforeEach(() => {
  previousHome = process.env.OPENCODEX_HOME;
  testHome = mkdtempSync(join(tmpdir(), "ocx-image-rotation-"));
  process.env.OPENCODEX_HOME = testHome;
  clearKeyCooldowns();
  resetProviderRequestPacingForTest();
});

afterEach(() => {
  clearKeyCooldowns();
  resetProviderRequestPacingForTest();
  if (previousHome === undefined) delete process.env.OPENCODEX_HOME;
  else process.env.OPENCODEX_HOME = previousHome;
  rmSync(testHome, { recursive: true, force: true });
});

function terminalChatSse(): Response {
  const chunk = {
    id: "chatcmpl-image-rotation",
    object: "chat.completion.chunk",
    created: 0,
    model: "grok",
    choices: [{ index: 0, delta: { role: "assistant", content: "done" }, finish_reason: null }],
  };
  const done = {
    ...chunk,
    choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
  };
  return new Response(
    [`data: ${JSON.stringify(chunk)}`, "", `data: ${JSON.stringify(done)}`, "", "data: [DONE]", "", ""].join("\n"),
    { headers: { "content-type": "text/event-stream" } },
  );
}

describe("image bridge provider rotation", () => {
  test("uses the freshly resolved provider transport after key rotation", async () => {
    const sends: Array<{ authorization: string | null; requestId: string | null }> = [];
    const transport = Object.assign(async (_input: Parameters<typeof fetch>[0], init?: RequestInit) => {
      const headers = new Headers(init?.headers);
      sends.push({
        authorization: headers.get("authorization"),
        requestId: headers.get("x-grok-req-id"),
      });
      return sends.length === 1
        ? Response.json({ error: { message: "rotate" } }, { status: 429 })
        : terminalChatSse();
    }, { preconnect() {} }) as typeof globalThis.fetch;
    const provider = {
      adapter: "openai-chat",
      baseUrl: "https://api.x.ai/v1",
      authMode: "key",
      apiKey: "fixture-key-a",
      apiKeyPool: [
        { id: "image-a", key: "fixture-key-a", addedAt: 1 },
        { id: "image-b", key: "fixture-key-b", addedAt: 2 },
      ],
      requestPacing: { enabled: true, minIntervalMs: 1 },
      fetch: transport,
    } as OcxProviderConfig & { fetch: typeof globalThis.fetch };
    const config = {
      port: 0,
      defaultProvider: "xai",
      providers: { xai: provider },
      images: { bridgeEnabled: true },
    } as OcxConfig;

    const response = await handleResponses(
      new Request("http://localhost/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          model: "xai/grok",
          stream: true,
          input: "hello",
          tools: [{ type: "image_generation" }],
        }),
      }),
      config,
      { model: "", provider: "" } as never,
      {},
    );
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("done");
    expect(sends.map(send => send.authorization)).toEqual([
      "Bearer fixture-key-a",
      "Bearer fixture-key-b",
    ]);
    expect(sends[0]?.requestId).toBeTruthy();
    expect(sends[1]?.requestId).toBeTruthy();
    expect(sends[1]?.requestId).not.toBe(sends[0]?.requestId);
  });
});
