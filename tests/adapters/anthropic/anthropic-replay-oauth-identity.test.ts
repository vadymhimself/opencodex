import { describe, expect, test } from "bun:test";
import { createAnthropicAdapter as createAnthropicAdapterProduction } from "../../../src/adapters/anthropic";
import { CLAUDE_CODE_SYSTEM_INSTRUCTION } from "../../../src/oauth/anthropic";
import { createTestTranslatorBudget, withTestTranslatorBudget } from "../../helpers/translator-budget";

const IDENTITY = { type: "text", text: CLAUDE_CODE_SYSTEM_INSTRUCTION };
const messages = [{ role: "user", content: "Reply with the word ok." }];

async function replay(system: unknown, authMode?: "oauth") {
  const adapter = withTestTranslatorBudget(createAnthropicAdapterProduction({
    adapter: "anthropic", baseUrl: "https://api.anthropic.com", apiKey: "key", ...(authMode ? { authMode } : {}),
  }));
  const source = { model: "claude-opus-4-1", max_tokens: 16, ...(system === undefined ? {} : { system }), messages };
  const request = await adapter.buildRequest({
    modelId: "claude-opus-4-1",
    context: { messages: [{ role: "user", content: "Reply with the word ok." }] },
    stream: false,
    options: { maxOutputTokens: 16 },
  }, { headers: new Headers(), translatorBudget: createTestTranslatorBudget(), anthropicMessagesSource: { body: source, headers: {} } });
  expect(request.anthropicSourceReplay).toBe(true);
  return JSON.parse(request.body);
}

describe("OAuth source replay carries the Claude identity block (ANDREW-CLAUDE-429)", () => {
  test("a Hermes-style string system gets the identity block first", async () => {
    expect(await replay("You are Hermes Agent.", "oauth")).toEqual({
      model: "claude-opus-4-1", max_tokens: 16, messages,
      system: [IDENTITY, { type: "text", text: "You are Hermes Agent." }],
    });
  });

  test("an array system keeps its blocks after the identity block", async () => {
    const block = { type: "text", text: "You are Hermes Agent.", cache_control: { type: "ephemeral" } };
    expect((await replay([block], "oauth")).system).toEqual([IDENTITY, block]);
  });

  test("no system becomes the identity block alone", async () => {
    expect((await replay(undefined, "oauth")).system).toEqual([IDENTITY]);
  });

  test("an already-identified Claude Code system is not doubled", async () => {
    const cc = [
      { type: "text", text: "x-anthropic-billing-header: cc_version=2.1.280" },
      { type: "text", text: "You are Claude Code, Anthropic's official CLI for Claude." },
      { type: "text", text: "project rules" },
    ];
    expect((await replay(cc, "oauth")).system).toEqual(cc);
    const sdk = [IDENTITY, { type: "text", text: "rules" }];
    expect((await replay(sdk, "oauth")).system).toEqual(sdk);
  });

  test("an API-key replay stays byte for byte", async () => {
    expect((await replay("You are Hermes Agent.")).system).toBe("You are Hermes Agent.");
  });
});
