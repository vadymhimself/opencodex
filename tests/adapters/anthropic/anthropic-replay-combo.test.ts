import { expect, test } from "bun:test";
import { canReplayAnthropicSource } from "../../../src/adapters/anthropic";
import type { OcxProviderConfig } from "../../../src/types";

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

// `system` is legally a plain STRING in the documented Anthropic shape, and on the replay lane
// the adapter reads the caller's raw body rather than one it built. Walking it as an array threw
// `blocks.map is not a function`, so every client sending the documented shape got a 500 instead
// of a replayed request. The cache-ttl scan must tolerate any shape the wire allows.
test("a string `system` does not crash the prompt-cache-ttl scan", async () => {
  const { effectivePromptCacheTtlMsForTest } = await import("../../../src/adapters/anthropic-cache-ttl-probe");
  for (const body of [
    { system: "You are a helpful assistant.", messages: [{ role: "user", content: "hi" }] },
    { system: [{ type: "text", text: "hi", cache_control: { type: "ephemeral" } }] },
    { tools: "not-an-array", messages: "not-an-array" },
    { messages: [null, "text", { content: "plain string" }] },
    {},
  ]) {
    expect(() => effectivePromptCacheTtlMsForTest(body as Record<string, unknown>)).not.toThrow();
  }
});
