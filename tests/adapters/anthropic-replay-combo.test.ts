import { expect, test } from "bun:test";
import { canReplayAnthropicSource } from "../../src/adapters/anthropic";
import type { OcxProviderConfig } from "../../src/types";

const provider = {
  adapter: "anthropic",
  baseUrl: "https://api.anthropic.com",
  authMode: "oauth",
} as OcxProviderConfig;

const withTool = (model: string) => ({
  model,
  max_tokens: 128,
  tools: [{ type: "web_fetch_20260209", name: "web_fetch" }],
  messages: [{ role: "user", content: "fetch a page" }],
});

// A combo alias names no source model, so a typed Anthropic tool must not fail closed:
// `combo/waterfall` never equals `claude-opus-5`, which rejected every WebFetch turn and
// surfaced as "No available targets" once the sibling leg was quota-capped.
test("combo alias keeps the Anthropic leg replayable for dated tool types", () => {
  expect(canReplayAnthropicSource(withTool("combo/waterfall"), "claude-opus-5", provider)).toBe(true);
  expect(canReplayAnthropicSource(withTool("combo/waterfall[1m]"), "claude-opus-5", provider)).toBe(true);
});

test("an explicit model change still fails closed on dated tool types", () => {
  expect(canReplayAnthropicSource(withTool("claude-haiku-4-5"), "claude-opus-5", provider)).toBe(false);
  expect(canReplayAnthropicSource(withTool("claude-opus-5"), "claude-opus-5", provider)).toBe(true);
});
