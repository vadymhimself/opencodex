import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { canReplayAnthropicSource } from "../../src/adapters/anthropic";
import { saveConfig } from "../../src/config";
import { providerConfigSeed } from "../../src/providers/derive";
import { PROVIDER_REGISTRY_CORE } from "../../src/providers/registry/entries-core";
import { startServer } from "../../src/server";
import type { OcxConfig } from "../../src/types";

const model = "claude-opus-5-5";
const providers = ["anthropic", "anthropic-apikey"] as const;
const originalFetch = globalThis.fetch;
const envKeys = ["HOME", "OPENCODEX_HOME", "CODEX_HOME", "CLAUDE_CONFIG_DIR", "OPENCODEX_CLAUDE_DESKTOP_CONFIG_DIR"] as const;
let previousEnv: Array<string | undefined>;
let home: string;

beforeEach(() => {
  previousEnv = envKeys.map(key => process.env[key]);
  home = mkdtempSync(join(tmpdir(), "ocx-opus-output-"));
  for (const key of envKeys) process.env[key] = home;
});

afterEach(() => {
  globalThis.fetch = originalFetch;
  envKeys.forEach((key, index) => {
    if (previousEnv[index] === undefined) delete process.env[key];
    else process.env[key] = previousEnv[index];
  });
  rmSync(home, { recursive: true, force: true });
});

// Sanitized Claude Code request shape: no captured prompts, account ids, or credentials.
function source(maxTokens = 128_000, serverTool = false) {
  return {
    model,
    max_tokens: maxTokens,
    stream: true,
    thinking: { type: "adaptive", display: "omitted" },
    output_config: { effort: "medium" },
    context_management: { edits: [{ type: "clear_thinking_20251015", keep: "all" }] },
    metadata: { user_id: "synthetic-replay-test" },
    system: [{ type: "text", text: "Synthetic system", cache_control: { type: "ephemeral" } }],
    messages: [
      { role: "user", content: [{ type: "text", text: "Synthetic prompt", cache_control: { type: "ephemeral" } }] },
      { role: "system", content: "Synthetic mid-conversation instruction" },
      { role: "user", content: "Continue" },
    ],
    tools: serverTool
      ? [{ type: "web_search_20250305", name: "web_search" }]
      : Array.from({ length: 12 }, (_, index) => ({
          name: `client_tool_${index}`,
          input_schema: { type: "object", properties: {} },
          ...(index === 11 ? { cache_control: { type: "ephemeral" } } : {}),
        })),
  };
}

const beta = "claude-code-20250219,interleaved-thinking-2025-05-14,thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05,mid-conversation-system-2026-04-07,per-turn-control-2026-07-01,mid-conversation-tool-changes-2026-07-01,advanced-tool-use-2025-11-20,effort-2025-11-24,context-1m-2025-08-07";
const sse = [
  { type: "message_start", message: { id: "msg_synthetic", type: "message", role: "assistant", model, content: [], stop_reason: null, usage: { input_tokens: 1, output_tokens: 0 } } },
  { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
  { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "synthetic success" } },
  { type: "content_block_stop", index: 0 },
  { type: "message_delta", delta: { stop_reason: "end_turn" }, usage: { output_tokens: 2 } },
  { type: "message_stop" },
].map(event => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`).join("");

for (const providerName of providers) {
  const entry = PROVIDER_REGISTRY_CORE.find(entry => entry.id === providerName)!;

  test(`${providerName} registered Opus 5.5 replay accepts 128000 without mutating the source`, () => {
    const provider = providerConfigSeed(entry);
    for (const maxTokens of [128_000, 64_000, 128]) {
      const body = source(maxTokens);
      const before = structuredClone(body);
      expect(canReplayAnthropicSource(body, model, provider)).toBe(true);
      expect(body).toEqual(before);
    }
    expect(canReplayAnthropicSource(source(128_001), model, provider)).toBe(false);
    expect(canReplayAnthropicSource(source(), model, {
      ...provider, modelMaxOutputTokens: { [model]: 64_000 },
    })).toBe(false);
    expect(canReplayAnthropicSource({ ...source(), model: "claude-haiku-4-5" }, "claude-haiku-4-5", provider)).toBe(false);
  });

  for (const route of ["direct", "combo"] as const) {
    for (const maxTokens of [128_000, 64_000]) {
      test(`${providerName} ${route} HTTP replay forwards ${maxTokens} unchanged and returns valid SSE`, async () => {
        const sends: string[] = [];
        globalThis.fetch = async (input, init) => {
          const request = new Request(input, init);
          expect(request.url).toBe("https://api.anthropic.com/v1/messages");
          expect(request.headers.get(providerName === "anthropic" ? "authorization" : "x-api-key"))
            .toBe(providerName === "anthropic" ? "Bearer synthetic-access" : "synthetic-key");
          for (const value of beta.split(",")) expect(request.headers.get("anthropic-beta")).toContain(value);
          sends.push(await request.text());
          return new Response(sse, { headers: { "content-type": "text/event-stream" } });
        };
        writeFileSync(join(home, "auth.json"), JSON.stringify({ anthropic: {
          activeAccountId: "synthetic-account",
          accounts: [{ id: "synthetic-account", credential: {
            access: "synthetic-access", refresh: "synthetic-refresh", expires: 9999999999999,
          } }],
        } }), { mode: 0o600 });
        saveConfig({
          port: 0,
          defaultProvider: providerName,
          // Minimal saved providers exercise registry enrichment, not hand-supplied caps.
          providers: { [providerName]: {
            adapter: entry.adapter, baseUrl: entry.baseUrl,
            ...(providerName === "anthropic" ? { authMode: "oauth" } : { authMode: "key", apiKey: "synthetic-key" }),
          } },
          ...(route === "combo" ? {
            combos: { opus: { strategy: "failover", targets: [{ provider: providerName, model }] } },
            claudeCode: { modelMap: { [model]: "combo/opus" } },
          } : {}),
        } as OcxConfig);
        const server = startServer(0);
        try {
          for (const serverTool of [false, true]) {
            const body = JSON.stringify(source(maxTokens, serverTool));
            const response = await originalFetch(new URL("/v1/messages?beta=true", server.url), {
              method: "POST",
              headers: { "content-type": "application/json", "anthropic-version": "2023-06-01", "anthropic-beta": beta, "x-api-key": "unused" },
              body,
            });
            expect(response.status).toBe(200);
            expect(await response.text()).toBe(sse);
            expect(sends.at(-1)).toBe(body);
            expect(JSON.parse(sends.at(-1)!).max_tokens).toBe(maxTokens);
          }
          expect(sends).toHaveLength(2);
        } finally {
          await server.stop(true);
        }
      });
    }
  }
}
