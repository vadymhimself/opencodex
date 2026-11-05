// Ported from AgentRadar images/ocx/pin.check.ts and failover.check.ts (the moved pin-patch rows): a real
// OCX process serving fake credentials against a loopback fake provider. No real tokens, no real provider.
import { afterAll, expect, test } from "bun:test";
import assert from "node:assert";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { removeTreeWithRetry } from "../helpers/remove-tree";

const CLI = join(import.meta.dir, "../../src/cli/index.ts");
const sha = (v: string) => createHash("sha256").update(v).digest("hex");

// --- fake provider: records every request OCX makes; 429s the tokens in `limited`, 403s (credential denial) those in `denied`.
type Seen = { path: string; auth: string | null; account: string | null; internal: string[]; search?: boolean; image?: boolean };
const seen: Seen[] = [];
const limited = new Set<string>();
const denied = new Set<string>();
const enc = new TextEncoder();
const sse = (events: [string, object][]) => new Response(new ReadableStream({
  start(c) { for (const [type, data] of events) c.enqueue(enc.encode(`event: ${type}\ndata: ${JSON.stringify({ type, ...data })}\n\n`)); c.close(); },
}), { headers: { "content-type": "text/event-stream" } });
const rateLimited = (retryAfter: string) => Response.json({ type: "error", error: { type: "rate_limit_error", message: "slow down" } }, { status: 429, headers: { "retry-after": retryAfter } });
let retryAfter = "30";
const fake = Bun.serve({
  port: 0,
  async fetch(req) {
    const path = new URL(req.url).pathname;
    const auth = req.headers.get("authorization");
    const body = await req.text();
    const tools: any[] = path === "/v1/messages" ? JSON.parse(body).tools ?? [] : [];
    // The helper's own call carries Anthropic's native search tool; the turn it serves carries OCX's web_search function.
    const search = tools.some(t => t.type === "web_search_20250305");
    if (path === "/v1/messages" || path === "/backend-api/codex/responses" || path.startsWith("/xai/") || path.startsWith("/kimi/")) {
      seen.push({ path, auth, account: req.headers.get("chatgpt-account-id"), internal: [...req.headers.keys()].filter(k => k.startsWith("x-agentradar-")), search, image: body.includes('"type":"image"') });
    }
    if (path === "/v1/messages") {
      if (auth && limited.has(auth)) return rateLimited(retryAfter);
      if (auth && denied.has(auth)) return Response.json({ type: "error", error: { type: "oauth_org_not_allowed",
        message: "Your organization has disabled Claude subscription access for Claude Code. Use an Anthropic API key instead, or ask your admin to enable access." } }, { status: 403 });
      if (!search && tools.some(t => t.name === "web_search") && !body.includes('"tool_result"')) return sse([
        ["message_start", { message: { id: "msg_tool", type: "message", role: "assistant", model: "claude-stub", content: [], stop_reason: null, usage: { input_tokens: 1, output_tokens: 0 } } }],
        ["content_block_start", { index: 0, content_block: { type: "tool_use", id: "toolu_fake", name: "web_search", input: {} } }],
        ["content_block_delta", { index: 0, delta: { type: "input_json_delta", partial_json: JSON.stringify({ query: "agentradar release notes" }) } }],
        ["content_block_stop", { index: 0 }],
        ["message_delta", { delta: { stop_reason: "tool_use", stop_sequence: null }, usage: { output_tokens: 1 } }],
        ["message_stop", {}],
      ]);
      return sse([
        ["message_start", { message: { id: "msg_fake", type: "message", role: "assistant", model: "claude-stub", content: [], stop_reason: null, usage: { input_tokens: 1, output_tokens: 0 } } }],
        ["content_block_start", { index: 0, content_block: { type: "text", text: "" } }],
        ["content_block_delta", { index: 0, delta: { type: "text_delta", text: "ok" } }],
        ["content_block_stop", { index: 0 }],
        ["message_delta", { delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 1 } }],
        ["message_stop", {}],
      ]);
    }
    if (path.startsWith("/xai/") || path.startsWith("/kimi/")) return rateLimited(retryAfter);
    if (path === "/backend-api/codex/responses") {
      const item = { type: "message", id: "msg_fake", role: "assistant", status: "completed", content: [{ type: "output_text", text: "ok", annotations: [] }] };
      const response = { id: "resp_fake", object: "response", model: "gpt-5.5", status: "in_progress", output: [] };
      return sse([
        ["response.created", { response }],
        ["response.output_text.delta", { item_id: item.id, output_index: 0, content_index: 0, delta: "ok" }],
        ["response.output_item.done", { output_index: 0, item }],
        ["response.completed", { response: { ...response, status: "completed", output: [item], usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 } } }],
      ]);
    }
    return Response.json({ error: "not found" }, { status: 404 });
  },
});
const FAKE = `http://127.0.0.1:${fake.port}`;
const cred = (access: string) => ({ access, refresh: `r-${access}`, expires: 9999999999999, email: `${access}@example.invalid` });
const codexCred = (n: number) => ({ credential: { accessToken: `cat${n}`, refreshToken: `crt${n}`, expiresAt: 9999999999999, chatgptAccountId: `cga${n}` }, generation: 0 });
const homes: string[] = [];

// One OCX process on its own home. Every non-loopback fetch (the Codex, Grok and Kimi backends) goes to the fake.
async function startOcx(auth: object, config: object) {
  const home = mkdtempSync(join(tmpdir(), "ocx-ar-pin-"));
  homes.push(home);
  mkdirSync(`${home}/.codex`, { mode: 0o700 });
  writeFileSync(`${home}/auth.json`, JSON.stringify(auth), { mode: 0o600 });
  writeFileSync(`${home}/codex-accounts.json`, JSON.stringify({ acct1: codexCred(1), acct2: codexCred(2) }), { mode: 0o600 });
  const probe = Bun.serve({ port: 0, fetch: () => new Response() });
  const port = probe.port;
  probe.stop(true);
  writeFileSync(`${home}/config.json`, JSON.stringify({ port, hostname: "127.0.0.1", ...config }), { mode: 0o600 });
  writeFileSync(`${home}/fake-net.ts`, `const real = globalThis.fetch;
globalThis.fetch = ((input: any, init?: any) => {
  const u = new URL(input instanceof Request ? input.url : String(input));
  if (u.hostname === "127.0.0.1" || u.hostname === "localhost") return real(input, init);
  const base = ${JSON.stringify(FAKE)} + (u.hostname === "api.x.ai" || u.hostname === "cli-chat-proxy.grok.com" ? "/xai" : u.hostname === "api.kimi.com" ? "/kimi" : "");
  return real(new Request(base + u.pathname + u.search, new Request(input, init)));
}) as typeof fetch;
`);
  const admin = `ocx_admin_${crypto.randomUUID()}${crypto.randomUUID()}`;
  const proc = Bun.spawn(["bun", "--preload", `${home}/fake-net.ts`, CLI, "start", "--port", String(port)], {
    env: { ...process.env, OPENCODEX_HOME: home, HOME: home, OPENCODEX_ADMIN_AUTH_TOKEN: admin }, stdout: "ignore", stderr: "pipe",
  });
  const ocx = { proc, home, admin, base: `http://127.0.0.1:${port}`, log: "" };
  (async () => { for await (const chunk of proc.stderr) ocx.log += new TextDecoder().decode(chunk); })();
  let lastError: unknown;
  for (let i = 0; ; i++) {
    try { if ((await fetch(`${ocx.base}/healthz`)).status < 500) break; } catch (error) { lastError = error; }
    if (i > 300) throw new Error(`OCX did not start (${String(lastError)}): ${ocx.log}`);
    await Bun.sleep(100);
  }
  return ocx;
}
type Ocx = Awaited<ReturnType<typeof startOcx>>;

const call = async (ocx: Ocx, path: string, body: object, headers: Record<string, string> = {}) => {
  const from = seen.length;
  const res = await fetch(`${ocx.base}${path}`, { method: "POST", headers: { "content-type": "application/json", ...headers }, body: JSON.stringify(body) });
  const text = await res.text();
  return { status: res.status, text, served: res.headers.get("x-agentradar-served-account-id"), upstream: seen.slice(from) };
};
const pin = (provider: string, id: string) => ({ "x-agentradar-account-provider": provider, "x-agentradar-account-id": id });
const msg = (ocx: Ocx, headers: Record<string, string> = {}, model = "claude-stub") => call(ocx, "/v1/messages",
  { model, max_tokens: 16, stream: true, messages: [{ role: "user", content: "hi" }] },
  { "anthropic-version": "2023-06-01", "x-api-key": "unused", ...headers });
const chat = (ocx: Ocx, model: string, headers: Record<string, string> = {}) => call(ocx, "/v1/chat/completions",
  { model, messages: [{ role: "user", content: "hi" }] }, headers);
const tokens = (r: { upstream: Seen[] }, prefix: string) => r.upstream.filter(s => s.path.startsWith(prefix)).map(s => s.auth);
const inference = (r: { upstream: Seen[] }) => r.upstream;
const accounts = async (ocx: Ocx, provider: string) => (await fetch(`${ocx.base}/api/oauth/accounts?provider=${provider}`, { headers: { authorization: `Bearer ${ocx.admin}` } })).json() as Promise<any>;

afterAll(() => {
  fake.stop(true);
  for (const home of homes) removeTreeWithRetry(home);
});

test("an AgentRadar account pin serves exactly its own stored account (pin.check)", async () => {
  limited.clear(); denied.clear(); retryAfter = "30";
  const ocx = await startOcx({
    anthropic: { activeAccountId: "B", accounts: [
      { id: "A", credential: cred("atA") }, { id: "B", credential: cred("atB") }, { id: "C", credential: cred("atC"), needsReauth: true },
      { id: "D", credential: cred("atD") }, { id: "E", credential: cred("atE") }] },
    xai: { activeAccountId: "gB", accounts: ["gA", "gB", "gC"].map(id => ({ id, credential: cred(`t${id}`) })) },
    kimi: { activeAccountId: "kB", accounts: ["kA", "kB"].map(id => ({ id, credential: cred(`t${id}`) })) },
  }, {
    defaultProvider: "anthropic", activeCodexAccountId: "acct2",
    clientIntegrations: { codex: false },
    // claude-blind cannot see images, so an image turn plans a vision describe through a helper model.
    visionSidecar: { backend: "routed", model: "anthropic/claude-stub" },
    providers: {
      anthropic: { adapter: "anthropic", baseUrl: FAKE, authMode: "oauth", allowPrivateNetwork: true, models: ["claude-stub", "claude-blind"], modelCapabilities: { "claude-blind": { inputModalities: ["text"] } } },
      xai: { adapter: "openai-chat", baseUrl: `${FAKE}/xai`, authMode: "oauth", allowPrivateNetwork: true, models: ["grok-stub"] },
      kimi: { adapter: "openai-chat", baseUrl: `${FAKE}/kimi`, authMode: "oauth", allowPrivateNetwork: true, models: ["kimi-stub"] },
      openai: { adapter: "openai-responses", baseUrl: "https://chatgpt.com/backend-api/codex", authMode: "forward", codexAccountMode: "pool" },
    },
    codexAccounts: [1, 2].map(n => ({ id: `acct${n}`, email: `c${n}@example.invalid`, isMain: false, chatgptAccountId: `cga${n}` })),
  });
  const rotations = () => ocx.log.split("[anthropic-pool] credential denial on").length - 1;
  const failovers429 = () => ocx.log.split("[anthropic-pool] 429 on").length - 1;
  try {
    // Claude: unpinned control serves the active account; a pin serves exactly its own, both ways, and names it.
    let r = await msg(ocx);
    assert.equal(r.status, 200, r.text); assert.deepEqual(tokens(r, "/v1/messages"), ["Bearer atB"]);
    assert.equal(r.served, "B");
    r = await msg(ocx, pin("claude", "A"));
    assert.equal(r.status, 200, r.text); assert.deepEqual(tokens(r, "/v1/messages"), ["Bearer atA"]);
    assert.equal(r.served, "A");
    r = await msg(ocx, pin("claude", "B"));
    assert.equal(r.status, 200, r.text); assert.deepEqual(tokens(r, "/v1/messages"), ["Bearer atB"]);
    // A caller Anthropic key would take native passthrough; a pin never does.
    r = await msg(ocx, { ...pin("claude", "A"), "x-api-key": "sk-ant-fake" });
    assert.equal(r.status, 200, r.text); assert.deepEqual(tokens(r, "/v1/messages"), ["Bearer atA"]);
    // Disconnected or missing pinned account: refused, never served by another account.
    for (const id of ["C", "missing"]) {
      r = await msg(ocx, pin("claude", id));
      assert.equal(r.status, 401, r.text); assert.deepEqual(inference(r), []);
      assert.equal(r.served, null, "an answer no account served names none");
    }
    // Malformed pins are refused, never served unpinned.
    for (const h of [pin("gemini", "A"), { "x-agentradar-account-id": "A" }, pin("claude", "has space")]) {
      r = await msg(ocx, h);
      assert.equal(r.status, 400, r.text); assert.deepEqual(inference(r), []);
      assert.equal(r.served, null);
    }
    // A pin cannot be spent on another provider's route.
    for (const h of [pin("grok", "gA"), pin("kimi", "kA"), pin("codex", "acct1")]) {
      r = await msg(ocx, h);
      assert.equal(r.status, 404, r.text); assert.deepEqual(inference(r), []);
    }
    // A helper model runs outside the pin (the loopback describe carries no pin): refused, nothing sent.
    const png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==";
    const blindImage = { model: "claude-blind", max_tokens: 16, stream: true, messages: [{ role: "user", content: [
      { type: "image", source: { type: "base64", media_type: "image/png", data: png } }, { type: "text", text: "what is this" }] }] };
    // Control: unpinned, the same turn really does plan a describe, so the refusal below is not vacuous.
    r = await call(ocx, "/v1/messages", blindImage, { "anthropic-version": "2023-06-01", "x-api-key": "unused" });
    assert.equal(r.status, 200, r.text);
    assert.ok(inference(r).length >= 2 && !inference(r).at(-1)!.image, `no vision describe planned: ${JSON.stringify(r.upstream)}`);
    r = await call(ocx, "/v1/messages", blindImage, { "anthropic-version": "2023-06-01", "x-api-key": "unused", ...pin("claude", "A") });
    assert.equal(r.status, 400, `${r.text}\n${JSON.stringify(r.upstream)}`); assert.deepEqual(inference(r), []);
    // Surfaces that do not honour a pin refuse it instead of serving the active account.
    r = await call(ocx, "/v1/responses", { model: "anthropic/claude-stub", input: "hi" }, pin("claude", "A"));
    assert.equal(r.status, 400, r.text); assert.match(r.text, /only honoured on/); assert.deepEqual(inference(r), []);
    // Claude Code's web search, as it sends it: the search helper runs on the pinned account too.
    r = await call(ocx, "/v1/messages", { model: "claude-stub", max_tokens: 64, stream: true,
      tools: [{ type: "web_search_20250305", name: "web_search", max_uses: 8 }],
      messages: [{ role: "user", content: "Search the web for the latest agentradar release notes" }] },
    { "anthropic-version": "2023-06-01", "anthropic-beta": "claude-code-20250219", "x-api-key": "unused", ...pin("claude", "A") });
    assert.equal(r.status, 200, r.text);
    assert.ok(r.upstream.some(s => s.search), `no search helper call: ${JSON.stringify(r.upstream)}`);
    assert.ok(r.upstream.filter(s => !s.search).length >= 2, `turn did not resume after the search: ${JSON.stringify(r.upstream)}`);
    assert.deepEqual([...new Set(tokens(r, "/v1/messages"))], ["Bearer atA"], JSON.stringify(r.upstream));
    // Grok and Codex have no helper that can run on their pin: still refused.
    r = await call(ocx, "/v1/chat/completions", { model: "xai/grok-stub", messages: [{ role: "user", content: "hi" }], tools: [{ type: "web_search" }] }, pin("grok", "gA"));
    assert.equal(r.status, 400, r.text); assert.match(r.text, /cannot use helper models/); assert.deepEqual(inference(r), []);
    assert.equal((await accounts(ocx, "anthropic")).activeAccountId, "B", "pins never move the active account");

    // Claude 429 on the pinned account: returned as is, cooldown recorded, no switch to B.
    limited.add("Bearer atA");
    r = await msg(ocx, pin("claude", "A"));
    assert.equal(r.status, 429, r.text); assert.deepEqual(tokens(r, "/v1/messages"), ["Bearer atA"]);
    // The rotator's own pin check: without it the caller picks another account and cancels this 429's body.
    assert.match(r.text, /slow down/);
    await Bun.sleep(200);
    assert.equal(failovers429(), 0, ocx.log);
    const claude = await accounts(ocx, "anthropic");
    assert.equal(claude.activeAccountId, "B");
    assert.equal(claude.accounts.find((a: any) => a.id === "A")?.health?.status, "cooldown", JSON.stringify(claude));
    r = await msg(ocx);
    assert.equal(r.status, 200, r.text); assert.deepEqual(tokens(r, "/v1/messages"), ["Bearer atB"]);

    // Claude credential denial on the pinned account: returned as is, never moved to E; the unpinned control does fail over.
    denied.add("Bearer atD");
    r = await msg(ocx, pin("claude", "D"));
    assert.equal(r.status, 403, r.text); assert.deepEqual(tokens(r, "/v1/messages"), ["Bearer atD"]);
    assert.equal((await accounts(ocx, "anthropic")).activeAccountId, "B");
    denied.add("Bearer atB");
    r = await msg(ocx);
    assert.equal(r.status, 200, r.text);
    const denialControl = tokens(r, "/v1/messages");
    assert.equal(denialControl[0], "Bearer atB"); assert.ok(denialControl.length > 1, `stock denial control did not rotate: ${denialControl}`);
    // The pinned 403's rotation stopped at the reauth mark: only the control's failover was logged.
    for (let i = 0; rotations() < 1 && i < 50; i++) await Bun.sleep(20);
    await Bun.sleep(200);
    assert.equal(rotations(), 1, ocx.log);

    // Grok (every call 429s): the pinned account alone, active unchanged; the unpinned control does rotate.
    r = await chat(ocx, "xai/grok-stub", pin("grok", "gA"));
    assert.equal(r.status, 429, `${r.text}\n${JSON.stringify(r.upstream)}`); assert.deepEqual(tokens(r, "/xai/"), ["Bearer tgA"]);
    assert.equal((await accounts(ocx, "xai")).activeAccountId, "gB");
    r = await chat(ocx, "xai/grok-stub");
    assert.equal(r.status, 429, r.text);
    const control = tokens(r, "/xai/");
    assert.equal(control[0], "Bearer tgB"); assert.ok(control.length > 1, `stock rotation control did not rotate: ${control}`);

    // Kimi (every call 429s): the pinned account alone, and the active account stays.
    r = await chat(ocx, "kimi/kimi-stub", pin("kimi", "kA"));
    assert.equal(r.status, 429, `${r.text}\n${JSON.stringify(r.upstream)}`); assert.deepEqual(tokens(r, "/kimi/"), ["Bearer tkA"]);
    assert.equal((await accounts(ocx, "kimi")).activeAccountId, "kB");
    r = await chat(ocx, "xai/grok-stub", pin("kimi", "kA"));
    assert.equal(r.status, 404, r.text); assert.deepEqual(inference(r), []);

    // Codex: OCX's exact-account route serves the pinned account; the active account is untouched.
    for (const n of [1, 2]) {
      r = await chat(ocx, "openai/gpt-5.5", pin("codex", `acct${n}`));
      assert.equal(r.status, 200, r.text);
      assert.deepEqual(r.upstream.filter(s => s.path === "/backend-api/codex/responses").map(s => [s.auth, s.account]), [[`Bearer cat${n}`, `cga${n}`]]);
      assert.equal(r.served, `acct${n}`, "a pinned chat answer names its account");
    }
    r = await chat(ocx, "openai/gpt-5.5", pin("codex", "missing"));
    assert.ok(r.status >= 400, r.text); assert.deepEqual(inference(r), []);
    const active = await (await fetch(`${ocx.base}/api/codex-auth/active`, { headers: { authorization: `Bearer ${ocx.admin}` } })).json() as any;
    assert.equal(active.activeCodexAccountId, "acct2", JSON.stringify(active));

    // The internal headers never reached any upstream request.
    assert.deepEqual(seen.filter(s => s.internal.length), []);
  } finally {
    ocx.proc.kill();
  }
}, 120_000);

test("pooled turns log and answer their served account hashed; one request walks every spent account (failover.check)", async () => {
  limited.clear(); denied.clear(); retryAfter = "318747";
  const ids = ["A", "B", "C", "D", "E", "F", "G", "H"];
  // The hosted gateway's pool defaults (AgentRadar images/ocx/pool-defaults.mjs).
  const ocx = await startOcx({
    anthropic: { activeAccountId: "A", accounts: ids.map(id => ({ id, credential: cred(`at${id}`) })) },
    xai: { activeAccountId: "gA", accounts: ids.map(id => ({ id: `g${id}`, credential: cred(`tg${id}`) })) },
  }, {
    defaultProvider: "anthropic", activeCodexAccountId: "acct1",
    anthropicAccountPool: { enabled: true, strategy: "round-robin" }, accountPoolStrategy: "round-robin", clientIntegrations: { codex: false },
    providers: {
      anthropic: { adapter: "anthropic", baseUrl: FAKE, authMode: "oauth", allowPrivateNetwork: true, models: ["claude-stub"] },
      xai: { adapter: "openai-chat", baseUrl: `${FAKE}/xai`, authMode: "oauth", allowPrivateNetwork: true, models: ["grok-stub"] },
    },
  });
  try {
    // CACHE-ACCOUNT-AFFINITY: each turn logs its served account and session hashed, and answers the account's id.
    const turns = [await msg(ocx, { "thread-id": "s1" }), await msg(ocx, { "thread-id": "s2" })];
    for (const t of turns) {
      assert.equal(t.status, 200, t.text);
      const servedId = ids.find(id => t.upstream[0]!.auth === `Bearer at${id}`)!;
      expect(t.served).toBe(servedId);
    }
    for (let i = 0; (ocx.log.match(/\[ar-acct\]/g) ?? []).length < 2 && i < 50; i++) await Bun.sleep(20);
    const logged = [...ocx.log.matchAll(/\[ar-acct\] a=([0-9a-f]{8}) r=(\S+) s=([0-9a-f]{8})\n/g)];
    expect(logged.map(m => m[1])).toEqual(turns.map(t => sha(t.served!).slice(0, 8)));
    expect(logged.map(m => m[3])).toEqual([sha("s1").slice(0, 8), sha("s2").slice(0, 8)]);
    expect(ocx.log).not.toContain("atA");

    // GR2 10-09: stock OCX gave up after 3 rotations. With every account answering 429, one Claude request
    // asks each account once; Grok's generic failover is bounded by OCX's shared request budget (5 sends).
    for (const id of ids) limited.add(`Bearer at${id}`).add(`Bearer tg${id}`);
    const claude = await msg(ocx, { "thread-id": "all-spent" });
    assert.equal(claude.status, 429, claude.text);
    expect([...tokens(claude, "/v1/messages")].sort()).toEqual(ids.map(id => `Bearer at${id}`));
    const grok = await msg(ocx, { "thread-id": "grok-all-spent" }, "xai/grok-stub");
    assert.equal(grok.status, 429, grok.text);
    expect(new Set(tokens(grok, "/xai/")).size).toBe(5);
  } finally {
    ocx.proc.kill();
  }
}, 120_000);
