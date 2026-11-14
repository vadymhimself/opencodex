import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { saveConfig } from "../../../src/config";
import { clearPoolRotationState } from "../../../src/codex/pool-rotation";
import {
  clearAnthropicAccountPoolState,
  getAnthropicAccountHealthSnapshot,
  rotateAnthropicAccountOn429,
} from "../../../src/oauth/anthropic-routing";
import { isPoolRotatableStatus, peekAnthropicPoolRotation } from "../../../src/oauth/anthropic-pool-rotation";
import { getAccountSet, saveCredential, setActiveAccount } from "../../../src/oauth/store";
import { clearRequestLogsForTests, getRequestLogEntries } from "../../../src/server/request-log";
import { startServer } from "../../../src/server";
import type { OcxConfig } from "../../../src/types";
import { installIsolatedCodexHome, type IsolatedCodexHome } from "../../helpers/isolated-codex-home";
import { removeTreeWithRetry } from "../../helpers/remove-tree";

// Anthropic answers a spent plan whose extra usage is also spent with HTTP 400, not 429. The pool
// used to rotate on 429 only, so one spent account failed the request while another sat idle.

const OUT_OF_EXTRA_USAGE = "You're out of extra usage. Add more at claude.ai/settings/usage and keep going.";
const THIRD_PARTY_EXTRA_USAGE = "Third-party apps now draw from your extra usage, which you have run out of.";
const ROLE_ORDER = "messages: roles must alternate between \"user\" and \"assistant\", but found multiple \"user\" roles in a row";

function anthropicError(status: number, type: string, message: string, headers?: Record<string, string>): Response {
  return Response.json({ type: "error", error: { type, message } }, { status, headers });
}

function anthropicTextSse(text: string): Response {
  return new Response([
    'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_test","type":"message","role":"assistant","model":"claude-opus-5","content":[],"stop_reason":null,"usage":{"input_tokens":3,"output_tokens":0}}}\n\n',
    `event: content_block_start\ndata: ${JSON.stringify({ type: "content_block_start", index: 0, content_block: { type: "text", text: "" } })}\n\n`,
    `event: content_block_delta\ndata: ${JSON.stringify({ type: "content_block_delta", index: 0, delta: { type: "text_delta", text } })}\n\n`,
    'event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n',
    `event: message_delta\ndata: ${JSON.stringify({ type: "message_delta", delta: { stop_reason: "end_turn" }, usage: { output_tokens: 1 } })}\n\n`,
    'event: message_stop\ndata: {"type":"message_stop"}\n\n',
  ].join(""), { headers: { "content-type": "text/event-stream" } });
}

let testDir = "";
let previousHome: string | undefined;
let isolatedCodexHome: IsolatedCodexHome | null = null;
const originalFetch = globalThis.fetch;

beforeEach(() => {
  previousHome = process.env.OPENCODEX_HOME;
  isolatedCodexHome = installIsolatedCodexHome("ocx-pool-400-");
  testDir = mkdtempSync(join(tmpdir(), "ocx-pool-400-"));
  process.env.OPENCODEX_HOME = testDir;
  clearRequestLogsForTests();
  clearAnthropicAccountPoolState();
  clearPoolRotationState();
  globalThis.fetch = originalFetch;
});

afterEach(() => {
  globalThis.fetch = originalFetch;
  clearAnthropicAccountPoolState();
  clearPoolRotationState();
  clearRequestLogsForTests();
  if (previousHome === undefined) delete process.env.OPENCODEX_HOME;
  else process.env.OPENCODEX_HOME = previousHome;
  isolatedCodexHome?.restore();
  isolatedCodexHome = null;
  if (testDir) removeTreeWithRetry(testDir);
});

describe("isPoolRotatableStatus", () => {
  test("429 always rotates", () => {
    expect(isPoolRotatableStatus(429)).toBe("429");
  });

  test("the two extra-usage 400 wordings rotate, enveloped or raw", () => {
    for (const message of [OUT_OF_EXTRA_USAGE, THIRD_PARTY_EXTRA_USAGE]) {
      const body = JSON.stringify({ type: "error", error: { type: "invalid_request_error", message } });
      expect(isPoolRotatableStatus(400, body)).toBe("quota-400");
      expect(isPoolRotatableStatus(400, message)).toBe("quota-400");
    }
  });

  test("a malformed-request 400 and other statuses do not rotate", () => {
    const body = JSON.stringify({ type: "error", error: { type: "invalid_request_error", message: ROLE_ORDER } });
    expect(isPoolRotatableStatus(400, body)).toBeNull();
    expect(isPoolRotatableStatus(400, "")).toBeNull();
    expect(isPoolRotatableStatus(400)).toBeNull();
    // The quota wording only counts on a 400; other statuses keep their own handling.
    expect(isPoolRotatableStatus(500, OUT_OF_EXTRA_USAGE)).toBeNull();
  });

  test("the peek hands back a response that still carries the whole 400 body", async () => {
    const peeked = await peekAnthropicPoolRotation(anthropicError(400, "invalid_request_error", ROLE_ORDER));
    expect(peeked.trigger).toBeNull();
    expect(peeked.response.status).toBe(400);
    expect(await peeked.response.text()).toContain("roles must alternate");
  });
});

describe("quota-400 cooldown", () => {
  async function seedTwoAccounts() {
    for (const id of ["a", "b"]) {
      await saveCredential("anthropic", {
        access: `access-${id}`,
        refresh: `refresh-${id}`,
        expires: Date.now() + 3_600_000,
        accountId: `uuid-${id}`,
        email: `${id}@example.test`,
      });
    }
    const set = getAccountSet("anthropic")!;
    const aId = set.accounts.find(acc => acc.credential.accountId === "uuid-a")!.id;
    const bId = set.accounts.find(acc => acc.credential.accountId === "uuid-b")!.id;
    await setActiveAccount("anthropic", aId);
    return { aId, bId };
  }
  const config = {
    port: 0,
    defaultProvider: "anthropic",
    providers: { anthropic: { adapter: "anthropic", baseUrl: "https://api.anthropic.com", authMode: "oauth" } },
    anthropicAccountPool: { enabled: true, autoSwitchThreshold: 100 },
  } as OcxConfig;

  test("without a reset it cools for the quota fallback, not the 60s rate-limit default", async () => {
    const { aId, bId } = await seedTwoAccounts();
    const now = 1_800_000_000_000;
    expect(rotateAnthropicAccountOn429(config, aId, null, "s", now, new Headers(), "claude-opus-5", "quota-400")).toBe(bId);
    expect(getAnthropicAccountHealthSnapshot(aId, now)?.cooldownUntil).toBe(now + 10 * 60_000);
    // Same call as a plain 429: unchanged 60s default.
    clearAnthropicAccountPoolState();
    expect(rotateAnthropicAccountOn429(config, aId, null, "s", now, new Headers(), "claude-opus-5")).toBe(bId);
    expect(getAnthropicAccountHealthSnapshot(aId, now)?.cooldownUntil).toBe(now + 60_000);
  });

  test("it cools even when the unified status still reads allowed, and honours a rejected window's reset", async () => {
    const { aId } = await seedTwoAccounts();
    const now = 1_800_000_000_000;
    const allowed = new Headers({ "anthropic-ratelimit-unified-status": "allowed" });
    rotateAnthropicAccountOn429(config, aId, null, "s", now, allowed, "claude-opus-5", "quota-400");
    expect(getAnthropicAccountHealthSnapshot(aId, now)?.cooldownUntil).toBe(now + 10 * 60_000);
    clearAnthropicAccountPoolState();
    const resetAt = now / 1000 + 3 * 3600;
    const rejected = new Headers({
      "anthropic-ratelimit-unified-status": "rejected",
      "anthropic-ratelimit-unified-5h-status": "rejected",
      "anthropic-ratelimit-unified-5h-reset": String(resetAt),
    });
    rotateAnthropicAccountOn429(config, aId, null, "s", now, rejected, "claude-opus-5", "quota-400");
    expect(getAnthropicAccountHealthSnapshot(aId, now)?.cooldownUntil).toBe(resetAt * 1000);
  });
});

describe("routed /v1/messages through the Anthropic pool", () => {
  function seedServer() {
    writeFileSync(join(testDir, "auth.json"), JSON.stringify({
      anthropic: {
        activeAccountId: "account-a",
        accounts: ["a", "b"].map(id => ({
          id: `account-${id}`,
          credential: {
            access: `synthetic-access-${id}`,
            refresh: `synthetic-refresh-${id}`,
            expires: 9999999999999,
            accountId: `synthetic-account-${id}`,
          },
        })),
      },
    }), { mode: 0o600 });
    // Hosted-gateway shape: no combo, the pool alone.
    saveConfig({
      port: 0,
      defaultProvider: "anthropic",
      providers: { anthropic: { adapter: "anthropic", baseUrl: "https://api.anthropic.com", authMode: "oauth" } },
      anthropicAccountPool: { enabled: true, autoSwitchThreshold: 100 },
    } as OcxConfig);
  }

  async function send(first: () => Response): Promise<{ response: Response; text: string; authorizations: Array<string | null> }> {
    const authorizations: Array<string | null> = [];
    globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input, init);
      if (request.url !== "https://api.anthropic.com/v1/messages") throw new Error(`unexpected egress ${request.url}`);
      authorizations.push(request.headers.get("authorization"));
      return authorizations.length === 1 ? first() : anthropicTextSse("rotated");
    }) as typeof fetch;
    seedServer();
    const server = startServer(0);
    try {
      const response = await originalFetch(new URL("/v1/messages", server.url), {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": "placeholder" },
        body: JSON.stringify({
          model: "claude-opus-5",
          max_tokens: 64,
          stream: true,
          messages: [{ role: "user", content: [{ type: "text", text: "hi" }] }],
        }),
      });
      return { response, text: await response.text(), authorizations };
    } finally {
      await server.stop(true);
    }
  }

  test("out-of-extra-usage 400 on account A retries on account B and succeeds", async () => {
    const { response, text, authorizations } = await send(
      () => anthropicError(400, "invalid_request_error", OUT_OF_EXTRA_USAGE),
    );
    expect(response.status).toBe(200);
    expect(text).toContain("rotated");
    expect(authorizations).toEqual(["Bearer synthetic-access-a", "Bearer synthetic-access-b"]);
    const entry = getRequestLogEntries().findLast(row => row.surface === "claude");
    // The log already maps an insufficient_quota failure to 429 (src/lib/errors.ts).
    expect(entry?.attempts).toHaveLength(2);
    expect(entry?.attempts?.[1]).toMatchObject({ status: 200, recoveryKinds: ["anthropic-oauth-429"] });
    expect(getAnthropicAccountHealthSnapshot("account-a")?.cooldownUntil).toBeGreaterThan(Date.now() + 9 * 60_000);
  });

  test("third-party-apps extra-usage 400 rotates too", async () => {
    const { response, text, authorizations } = await send(
      () => anthropicError(400, "invalid_request_error", THIRD_PARTY_EXTRA_USAGE),
    );
    expect(response.status).toBe(200);
    expect(text).toContain("rotated");
    expect(authorizations).toEqual(["Bearer synthetic-access-a", "Bearer synthetic-access-b"]);
  });

  test("a malformed-request 400 does not rotate and reaches the caller", async () => {
    const { response, text, authorizations } = await send(
      () => anthropicError(400, "invalid_request_error", ROLE_ORDER),
    );
    expect(response.status).toBe(400);
    expect(text).toContain("roles must alternate");
    expect(authorizations).toEqual(["Bearer synthetic-access-a"]);
    expect(getAnthropicAccountHealthSnapshot("account-a")).toBeNull();
  });
});
