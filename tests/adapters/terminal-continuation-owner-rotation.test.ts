import { afterEach, beforeEach, describe, expect, mock, test } from "bun:test";
import { mkdtempSync} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ProviderAdapter } from "../../src/adapters/base";
import { saveConfig } from "../../src/config";
import { clearKeyCooldowns } from "../../src/providers/key-failover";
import { clearGenericFailoverHealth } from "../../src/oauth/generic-account-failover";
import { getValidAccessSnapshotForAccount } from "../../src/oauth";
import { getAccountSet, saveCredential } from "../../src/oauth/store";
import { reasoningReplayKeyCredentialIdentity, reasoningReplayOAuthCredentialIdentity } from "../../src/responses/reasoning-replay-cache";
import type { RequestLogContext } from "../../src/server/request-log";
import {
  clearResponseStateForTests,
  previousResponseProviderState,
} from "../../src/responses/state";
import type {
  AdapterEvent,
  OcxConfig,
  OcxParsedRequest,
  OcxProviderConfig,
} from "../../src/types";
import { removeTreeWithRetry } from "../helpers/remove-tree";

interface BuildObservation {
  key: string;
  continuation?: string;
}

let builds: BuildObservation[] = [];
let failContinuationBuild = false;

const PLAN_USAGE = {
  inputTokens: 10,
  outputTokens: 2,
  contextTotalTokens: 100,
  anthropicServerToolUse: { web_search_requests: 1 },
};
const ROTATED_USAGE = {
  inputTokens: 20,
  outputTokens: 3,
  contextTotalTokens: 140,
  anthropicServerToolUse: { web_search_requests: 2, web_fetch_requests: 1 },
};

function eventsForPhase(phase: string): AdapterEvent[] {
  if (phase === "seed") {
    return [
      { type: "text_delta", text: "seeded" },
      {
        type: "done",
        stopReason: "end_turn",
        providerState: { kiro: { conversationId: "private-a" } },
      },
    ];
  }
  if (phase === "plan") {
    return [
      { type: "text_delta", text: "I will modify the file now." },
      {
        type: "done",
        stopReason: "end_turn",
        usage: PLAN_USAGE,
        providerState: { kiro: { conversationId: "private-a-plan" } },
      },
    ];
  }
  if (phase === "rotated") {
    return [
      { type: "text_delta", text: "completed on the rotated key" },
      {
        type: "done",
        stopReason: "end_turn",
        usage: ROTATED_USAGE,
        providerState: { kiro: { conversationId: "private-b" } },
      },
    ];
  }
  if (phase === "follow") {
    return [
      { type: "text_delta", text: "continued on the rotated key" },
      {
        type: "done",
        stopReason: "end_turn",
        providerState: { kiro: { conversationId: "private-b-next" } },
      },
    ];
  }
  if (phase === "follow-active") {
    return [
      { type: "text_delta", text: "continued on the active account" },
      {
        type: "done",
        stopReason: "end_turn",
        providerState: { kiro: { conversationId: "private-a-next" } },
      },
    ];
  }
  throw new Error(`unexpected test phase: ${phase}`);
}

const actualResolver = await import("../../src/server/adapter-resolve");
const actualResolveAdapter = actualResolver.resolveAdapter;

mock.module("../../src/server/adapter-resolve", () => ({
  ...actualResolver,
  resolveAdapter(provider: OcxProviderConfig, cacheRetention?: "none" | "short" | "long") {
    const key = provider.apiKey ?? "";
    if (
      provider.adapter !== "test-terminal-owned"
      && !key.startsWith("cursor-access-")
    ) {
      return actualResolveAdapter(provider, cacheRetention);
    }
    const adapter: ProviderAdapter = {
      // The terminal guard is enabled for Anthropic adapters. The transport is otherwise a
      // narrow test double so the test can emit provider-private state deterministically.
      name: "anthropic",
      buildRequest(parsed: OcxParsedRequest) {
        const continuation = parsed._providerContinuation?.kiro?.conversationId;
        builds.push({ key, ...(continuation ? { continuation } : {}) });
        if (failContinuationBuild && parsed.context.messages.at(-1)?.role === "developer") {
          throw new Error("continuation build failed");
        }
        return {
          url: "https://owned-terminal.test/v1/messages",
          method: "POST",
          headers: { authorization: `Bearer ${key}` },
          body: "{}",
        };
      },
      async *parseStream(response: Response): AsyncGenerator<AdapterEvent> {
        yield* eventsForPhase(response.headers.get("x-test-phase") ?? "");
      },
      async parseResponse(response: Response): Promise<AdapterEvent[]> {
        return eventsForPhase(response.headers.get("x-test-phase") ?? "");
      },
    };
    return adapter;
  },
}));

const { handleResponses } = await import("../../src/server/responses");

describe("terminal continuation provider-owner rotation", () => {
  let originalFetch: typeof fetch;
  let previousHome: string | undefined;
  let testHome = "";

  beforeEach(() => {
    originalFetch = globalThis.fetch;
    previousHome = process.env.OPENCODEX_HOME;
    testHome = mkdtempSync(join(tmpdir(), "ocx-terminal-owner-"));
    process.env.OPENCODEX_HOME = testHome;
    builds = [];
    failContinuationBuild = false;
    clearGenericFailoverHealth();
    clearKeyCooldowns();
    clearResponseStateForTests();
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    if (previousHome === undefined) delete process.env.OPENCODEX_HOME;
    else process.env.OPENCODEX_HOME = previousHome;
    clearGenericFailoverHealth();
    clearKeyCooldowns();
    clearResponseStateForTests();
    removeTreeWithRetry(testHome);
  });

  test("429 rotation fences inherited state and persists the rotated owner", async () => {
    const keyA = "key-alpha-000111222333";
    const keyB = "key-beta-444555666777";
    const config: OcxConfig = {
      port: 0,
      defaultProvider: "owned",
      providers: {
        owned: {
          adapter: "test-terminal-owned",
          baseUrl: "https://owned-terminal.test/v1",
          authMode: "key",
          apiKey: keyA,
          apiKeyPool: [
            { id: "k1", key: keyA, addedAt: 1 },
            { id: "k2", key: keyB, addedAt: 2 },
          ],
        },
      },
    } as OcxConfig;
    const keyAIdentity = reasoningReplayKeyCredentialIdentity({ apiKey: keyA });
    saveConfig(config);

    const phases = ["seed", "plan", "rate-limit", "rotated", "follow"];
    const seenAuthorization: string[] = [];
    globalThis.fetch = (async (_input, init) => {
      seenAuthorization.push(new Headers(init?.headers).get("authorization") ?? "");
      const phase = phases.shift();
      if (!phase) throw new Error("unexpected extra upstream request");
      if (phase === "rate-limit") {
        return Response.json(
          { error: { message: "rotate" } },
          { status: 429, headers: { "retry-after": "30" } },
        );
      }
      return new Response("", { headers: { "x-test-phase": phase } });
    }) as typeof fetch;

    const post = (
      body: Record<string, unknown>,
      logCtx: RequestLogContext = { model: "", provider: "" },
    ) => handleResponses(
      new Request("http://localhost/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      }),
      config,
      logCtx,
    );

    const seed = await post({
      model: "owned/model",
      input: "seed",
      stream: false,
      store: true,
    });
    expect(seed.status).toBe(200);
    const seedJson = await seed.json() as { id: string };
    expect(previousResponseProviderState(seedJson.id)).toMatchObject({
      __ocxOwner: { credentialIdentity: keyAIdentity },
      kiro: { conversationId: "private-a" },
    });

    const rotatedLogCtx: RequestLogContext = { model: "", provider: "" };
    const rotated = await post({
      model: "owned/model",
      previous_response_id: seedJson.id,
      input: "Please modify the file now",
      stream: false,
      store: true,
      tools: [{
        type: "function",
        name: "read_file",
        description: "read a file",
        parameters: { type: "object" },
      }],
    }, rotatedLogCtx);
    expect(rotated.status).toBe(200);
    const rotatedJson = await rotated.json() as { id: string };
    const keyBIdentity = reasoningReplayKeyCredentialIdentity(config.providers.owned!);
    expect(keyBIdentity).toBeDefined();
    expect(keyBIdentity).not.toBe(keyAIdentity);
    expect(previousResponseProviderState(rotatedJson.id)).toMatchObject({
      __ocxOwner: { credentialIdentity: keyBIdentity },
      kiro: { conversationId: "private-b" },
    });
    expect(rotatedLogCtx.attempts).toMatchObject([
      {
        status: 429,
        sendCount: 2,
        usage: PLAN_USAGE,
      },
      {
        sendCount: 1,
        usage: ROTATED_USAGE,
      },
    ]);
    expect(rotatedLogCtx.usage).toMatchObject({
      inputTokens: 30,
      outputTokens: 5,
      totalTokens: 35,
      contextTotalTokens: 140,
      anthropicServerToolUse: { web_search_requests: 3, web_fetch_requests: 1 },
    });

    const follow = await post({
      model: "owned/model",
      previous_response_id: rotatedJson.id,
      input: "follow up",
      stream: false,
      store: true,
    });
    expect(follow.status).toBe(200);
    const followJson = await follow.json() as { id: string };
    expect(previousResponseProviderState(followJson.id)).toMatchObject({
      __ocxOwner: { credentialIdentity: keyBIdentity },
      kiro: { conversationId: "private-b-next" },
    });

    expect(seenAuthorization).toEqual([
      `Bearer ${keyA}`,
      `Bearer ${keyA}`,
      `Bearer ${keyA}`,
      `Bearer ${keyB}`,
      `Bearer ${keyB}`,
    ]);
    expect(builds).toEqual([
      { key: keyA },
      { key: keyA, continuation: "private-a" },
      { key: keyA, continuation: "private-a" },
      { key: keyB },
      { key: keyB, continuation: "private-b" },
    ]);
    expect(phases).toEqual([]);
  });

  test("a continuation build failure preserves the completed physical attempt", async () => {
    const config: OcxConfig = {
      port: 0,
      defaultProvider: "owned",
      providers: {
        owned: {
          adapter: "test-terminal-owned",
          baseUrl: "https://owned-terminal.test/v1",
          authMode: "key",
          apiKey: "key-alpha-000111222333",
        },
      },
    } as OcxConfig;
    saveConfig(config);
    failContinuationBuild = true;
    const phases = ["plan"];
    globalThis.fetch = (async () => {
      const phase = phases.shift();
      if (!phase) throw new Error("unexpected extra upstream request");
      return new Response("", { headers: { "x-test-phase": phase } });
    }) as typeof fetch;
    const logCtx: RequestLogContext = { model: "", provider: "" };

    const failed = await handleResponses(
      new Request("http://localhost/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          model: "owned/model",
          input: "Please modify the file now",
          stream: false,
          tools: [{
            type: "function",
            name: "read_file",
            description: "read a file",
            parameters: { type: "object" },
          }],
        }),
      }),
      config,
      logCtx,
    );

    expect(await failed.json()).toMatchObject({
      status: "failed",
      error: { message: "Provider continuation failed: continuation build failed" },
    });
    expect(logCtx.attempts).toHaveLength(1);
    expect(logCtx.attempts?.[0]).toMatchObject({
      sendCount: 1,
      usage: PLAN_USAGE,
    });
    expect(phases).toEqual([]);
  });

  test("generic OAuth rotation fences state when the next request returns to the active account", async () => {
    for (let index = 0; index < 2; index += 1) {
      await saveCredential("nous", {
        access: `cursor-access-${index}`,
        refresh: `cursor-refresh-${index}`,
        expires: Date.now() + 3_600_000,
        accountId: `cursor-account-${index}`,
      }, { addAccount: true });
    }
    const accountIds = getAccountSet("nous")?.accounts.map(account => account.id) ?? [];
    const snapshotA = await getValidAccessSnapshotForAccount("nous", accountIds[1]!);
    const snapshotB = await getValidAccessSnapshotForAccount("nous", accountIds[0]!);
    const ownerA = reasoningReplayOAuthCredentialIdentity(snapshotA);
    const ownerB = reasoningReplayOAuthCredentialIdentity(snapshotB);
    const config: OcxConfig = {
      port: 0,
      defaultProvider: "nous",
      providers: {
        nous: {
          adapter: "test-terminal-owned",
          baseUrl: "https://owned-terminal.test/v1",
          authMode: "oauth",
          models: ["model"],
        },
      },
    } as OcxConfig;
    saveConfig(config);

    const phases = ["seed", "plan", "rate-limit", "rotated", "follow-active"];
    const seenAuthorization: string[] = [];
    globalThis.fetch = (async (_input, init) => {
      seenAuthorization.push(new Headers(init?.headers).get("authorization") ?? "");
      const phase = phases.shift();
      if (!phase) throw new Error("unexpected extra upstream request");
      if (phase === "rate-limit") {
        return Response.json(
          { error: { message: "rotate" } },
          { status: 429, headers: { "retry-after": "30" } },
        );
      }
      return new Response("", { headers: { "x-test-phase": phase } });
    }) as typeof fetch;

    const post = (
      body: Record<string, unknown>,
      logCtx: RequestLogContext = { model: "", provider: "" },
    ) => handleResponses(
      new Request("http://localhost/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      }),
      config,
      logCtx,
    );

    const seed = await post({
      model: "nous/model",
      input: "seed",
      stream: false,
      store: true,
    });
    expect(seed.status).toBe(200);
    const seedJson = await seed.json() as { id: string };
    expect(previousResponseProviderState(seedJson.id)).toMatchObject({
      __ocxOwner: { credentialIdentity: ownerA },
      kiro: { conversationId: "private-a" },
    });

    const rotatedLogCtx: RequestLogContext = { model: "", provider: "" };
    const rotated = await post({
      model: "nous/model",
      previous_response_id: seedJson.id,
      input: "Please modify the file now",
      stream: false,
      store: true,
      tools: [{
        type: "function",
        name: "read_file",
        description: "read a file",
        parameters: { type: "object" },
      }],
    }, rotatedLogCtx);
    expect(rotated.status).toBe(200);
    const rotatedJson = await rotated.json() as { id: string };
    expect(ownerB).toBeDefined();
    expect(ownerB).not.toBe(ownerA);
    expect(previousResponseProviderState(rotatedJson.id)).toMatchObject({
      __ocxOwner: { credentialIdentity: ownerB },
      kiro: { conversationId: "private-b" },
    });
    expect(rotatedLogCtx.attempts).toMatchObject([
      { status: 429, sendCount: 2 },
      { sendCount: 1, recoveryKinds: ["oauth-account-429"] },
    ]);

    const follow = await post({
      model: "nous/model",
      previous_response_id: rotatedJson.id,
      input: "follow up",
      stream: false,
      store: true,
    });
    expect(follow.status).toBe(200);
    const followJson = await follow.json() as { id: string };
    expect(previousResponseProviderState(followJson.id)).toMatchObject({
      __ocxOwner: { credentialIdentity: ownerA },
      kiro: { conversationId: "private-a-next" },
    });

    expect(seenAuthorization).toEqual([
      "Bearer cursor-access-1",
      "Bearer cursor-access-1",
      "Bearer cursor-access-1",
      "Bearer cursor-access-0",
      "Bearer cursor-access-1",
    ]);
    expect(builds).toEqual([
      { key: "cursor-access-1" },
      { key: "cursor-access-1", continuation: "private-a" },
      { key: "cursor-access-1", continuation: "private-a" },
      { key: "cursor-access-0" },
      { key: "cursor-access-1" },
    ]);
    expect(phases).toEqual([]);
  });
});
