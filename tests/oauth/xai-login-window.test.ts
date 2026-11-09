import { describe, expect, test } from "bun:test";
import { XAI_LOGIN_TIMEOUT_MS, XaiOAuthFlow } from "../../src/oauth/xai";

// AgentRadar 20115: a Grok paste 5-6 minutes after the start failed because the flow kept the 300 s default.
describe("xAI login window", () => {
  test("waits 20 minutes for the callback or a pasted address", () => {
    const flow = new XaiOAuthFlow({ onAuth: () => {} } as never);
    expect(XAI_LOGIN_TIMEOUT_MS).toBe(20 * 60_000);
    expect(flow.timeoutMs).toBe(XAI_LOGIN_TIMEOUT_MS);
  });
});
