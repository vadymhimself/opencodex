#!/usr/bin/env bun

/**
 * Session-level native-versus-gateway quota parity.
 *
 * The HTTP-level eval (`claude-cache-count-parity-eval.ts`) proves one request pair. This one
 * proves the thing the quota bill actually reflects: a real Claude Code session doing real tool
 * work, measured from the SAME source for both arms — Claude Code's own transcript usage rows —
 * so no gateway-side accounting can flatter the gateway arm.
 *
 * Isolation rules, non-negotiable:
 * - The gateway runs from a throwaway OPENCODEX_HOME on an ephemeral port. It never reads or
 *   writes the live `~/.opencodex` state, and never touches the laptop or Mini gateways.
 * - `ANTHROPIC_BASE_URL` is set only in the child session's environment. Nothing repoints
 *   existing sessions (the `ocx start` incident of 2026-09-01).
 * - No credential, prompt, or account identifier is printed. Only token counts and shares.
 */

import { execFile } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { createConnection, createServer } from "node:net";
import { homedir, tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const LIVE_OPT_IN = "I_ACCEPT_ANTHROPIC_QUOTA_USAGE";
const GATEWAY_READY_TIMEOUT_MS = 30_000;
const SESSION_TIMEOUT_MS = 600_000;
/** Warmed turns must reuse at least this share of their input, on either arm. */
const WARM_READ_SHARE_MIN = 0.9;
/** How far the gateway arm's warmed read share may fall below the native arm's. */
const READ_SHARE_DELTA_MAX = 0.05;

type TurnUsage = {
  index: number;
  inclusive: number;
  raw: number;
  read: number;
  write: number;
  readShare: number;
};

type ArmResult = {
  arm: "native" | "gateway";
  model: string;
  turns: TurnUsage[];
  totals: { inclusive: number; raw: number; read: number; write: number };
  warmedReadShare: number | null;
  maxRaw: number;
  maxWarmWrite: number;
};

function invariant(condition: unknown, code: string): asserts condition {
  if (!condition) throw new Error(code);
}

async function freeLoopbackPort(): Promise<number> {
  return await new Promise((resolvePort, reject) => {
    const probe = createServer();
    probe.on("error", reject);
    probe.listen({ host: "127.0.0.1", port: 0 }, () => {
      const address = probe.address();
      invariant(address !== null && typeof address === "object", "ephemeral_port_unavailable");
      probe.close(error => (error ? reject(error) : resolvePort(address.port)));
    });
  });
}

async function portAcceptsConnections(port: number): Promise<boolean> {
  return await new Promise(resolvePort => {
    const socket = createConnection({ host: "127.0.0.1", port });
    const settle = (value: boolean) => {
      socket.destroy();
      resolvePort(value);
    };
    socket.once("connect", () => settle(true));
    socket.once("error", () => settle(false));
    setTimeout(() => settle(false), 1_000);
  });
}

/**
 * Copy only what an isolated gateway needs to route: the config and the credential store. The
 * copy is never read into this process, never logged, and lives under a throwaway directory.
 */
function seedIsolatedHome(target: string, siblings: string[]): void {
  mkdirSync(target, { recursive: true });
  // CODEX_HOME and the Claude config dirs must EXIST before startup: the resolver refuses an
  // unreadable path rather than creating it, and refusing is correct — a typo there would
  // otherwise silently fall back to the real home.
  for (const sibling of siblings) mkdirSync(sibling, { recursive: true });
  const source = join(homedir(), ".opencodex");
  for (const name of ["config.json", "auth.json"]) {
    const from = join(source, name);
    invariant(existsSync(from), `missing_${name.replace(".", "_")}`);
    copyFileSync(from, join(target, name));
  }
  // `claudeCode.systemEnv` makes startup run `launchctl setenv ANTHROPIC_BASE_URL` for the whole
  // LOGIN SESSION, repointing every existing Claude Code process at this throwaway port. No
  // environment variable can contain that, and the usual "another instance owns env" guard reads
  // its tracking file from the config dir — which is this throwaway home, so it always looks
  // unowned. It has to be disabled in the copy itself.
  const configPath = join(target, "config.json");
  const config = JSON.parse(readFileSync(configPath, "utf8")) as Record<string, unknown>;
  const claudeCode = (config.claudeCode ?? {}) as Record<string, unknown>;
  claudeCode.systemEnv = false;
  config.claudeCode = claudeCode;
  writeFileSync(configPath, JSON.stringify(config, null, 2));
}

function transcriptDir(): string {
  const encoded = process.cwd().replaceAll("/", "-");
  return join(homedir(), ".claude", "projects", encoded);
}

function transcriptNames(dir: string): Set<string> {
  return existsSync(dir)
    ? new Set(readdirSync(dir).filter(name => name.endsWith(".jsonl")))
    : new Set();
}

/**
 * The transcript this run created, identified as the file that did not exist before it started.
 * "Newest mtime" is not an identity check: any other Claude Code session in the same cwd appends
 * to its own transcript, and one write landing after ours would silently hand this eval a
 * different session's usage — wrong numbers AND a wrong verdict, with nothing to signal it.
 */
function newTranscript(dir: string, before: Set<string>): string {
  invariant(existsSync(dir), "claude_project_dir_missing");
  const created = readdirSync(dir)
    .filter(name => name.endsWith(".jsonl") && !before.has(name))
    .map(name => ({ path: join(dir, name), mtime: statSync(join(dir, name)).mtimeMs }))
    .sort((a, b) => b.mtime - a.mtime);
  invariant(created.length > 0, "transcript_not_found");
  return created[0]!.path;
}

/**
 * Claude Code records one usage object per assistant message, and repeats it across the content
 * blocks of a single call. Deduplicate on `message.id` or the run would count one call many times
 * (the AgentRadar 210-versus-183 correction).
 */
function transcriptUsage(path: string): TurnUsage[] {
  const seen = new Set<string>();
  const turns: TurnUsage[] = [];
  for (const line of readFileSync(path, "utf8").split("\n")) {
    if (!line.trim()) continue;
    let row: Record<string, unknown>;
    try {
      row = JSON.parse(line) as Record<string, unknown>;
    } catch {
      continue;
    }
    if (row.type !== "assistant" || row.isSidechain === true) continue;
    const message = row.message as Record<string, unknown> | undefined;
    const id = typeof message?.id === "string" ? message.id : undefined;
    const usage = message?.usage as Record<string, unknown> | undefined;
    if (!id || !usage || seen.has(id)) continue;
    seen.add(id);
    const input = Number(usage.input_tokens ?? 0);
    const read = Number(usage.cache_read_input_tokens ?? 0);
    const write = Number(usage.cache_creation_input_tokens ?? 0);
    if (!Number.isFinite(input) || !Number.isFinite(read) || !Number.isFinite(write)) continue;
    // Anthropic reports `input_tokens` EXCLUSIVE of cache counters on the Messages API, so the
    // inclusive figure the ledger uses is the sum. Mixing the two conventions is the single
    // easiest way to fake a passing parity run.
    const inclusive = input + read + write;
    if (inclusive <= 0) continue;
    turns.push({
      index: turns.length,
      inclusive,
      raw: input,
      read,
      write,
      readShare: read / inclusive,
    });
  }
  return turns;
}

function summarize(arm: "native" | "gateway", model: string, turns: TurnUsage[]): ArmResult {
  const totals = turns.reduce(
    (acc, turn) => ({
      inclusive: acc.inclusive + turn.inclusive,
      raw: acc.raw + turn.raw,
      read: acc.read + turn.read,
      write: acc.write + turn.write,
    }),
    { inclusive: 0, raw: 0, read: 0, write: 0 },
  );
  // Turn 0 writes its prefix cold, and turn 1 still misses whenever this arm's prefix cohort is
  // itself new — the gateway arm runs from a throwaway home, so that is every run. Measuring from
  // turn 2 compares steady state on both arms instead of scoring the gateway on being new.
  // Verified 2026-09-15: a gateway arm whose cohort was already warm scored 0.9623 against
  // native's 0.9623, while its first-ever run scored 0.8522 on identical work.
  const warmed = turns.slice(2);
  const warmedInclusive = warmed.reduce((sum, turn) => sum + turn.inclusive, 0);
  const warmedRead = warmed.reduce((sum, turn) => sum + turn.read, 0);
  return {
    arm,
    model,
    turns,
    totals,
    warmedReadShare: warmedInclusive > 0 ? warmedRead / warmedInclusive : null,
    maxRaw: turns.reduce((max, turn) => Math.max(max, turn.raw), 0),
    maxWarmWrite: warmed.reduce((max, turn) => Math.max(max, turn.write), 0),
  };
}

/**
 * Set `OPENCODEX_PARITY_TOOL_SEARCH=0` to measure the unsupported configuration instead — useful
 * for reproducing the inflation itself, not for judging the gateway.
 */
const toolSearch = process.env.OPENCODEX_PARITY_TOOL_SEARCH !== "0";

async function runSession(options: {
  arm: "native" | "gateway";
  model: string;
  prompt: string;
  baseUrl?: string;
}): Promise<ArmResult> {
  const dir = transcriptDir();
  const before = transcriptNames(dir);
  // A developer shell is commonly ALREADY pointed at a live gateway via ANTHROPIC_BASE_URL plus a
  // gateway ANTHROPIC_AUTH_TOKEN. Clearing only the base URL sends that gateway token to
  // api.anthropic.com and 401s; leaving both silently measures gateway-versus-gateway and calls
  // it parity. The native arm must fall back to Claude Code's own stored login.
  const env = { ...process.env };
  delete env.ANTHROPIC_AUTH_TOKEN;
  delete env.ANTHROPIC_API_KEY;
  if (options.baseUrl) {
    env.ANTHROPIC_BASE_URL = options.baseUrl;
    // Claude Code defers MCP tools by default but turns tool search OFF for a non-first-party
    // base URL, because most proxies drop `tool_reference`. Ours does not (strict replay forwards
    // the body and relays Anthropic's bytes verbatim), so the supported configuration is tool
    // search ON — measured 103,158 -> 33,984 turn-0 tokens. Without this the eval would measure a
    // setup nobody should run and report its 2.4x as if it were a gateway regression.
    env.ENABLE_TOOL_SEARCH = toolSearch ? "true" : "false";
  } else {
    delete env.ANTHROPIC_BASE_URL;
  }
  try {
    await execFileAsync(
      "claude",
      ["-p", options.prompt, "--model", options.model, "--permission-mode", "acceptEdits"],
      { env, timeout: SESSION_TIMEOUT_MS, maxBuffer: 64 * 1024 * 1024, cwd: process.cwd() },
    );
  } catch (error) {
    // Node builds `err.message` as "Command failed: " + argv.join(" ") + stderr, and the prompt is
    // an argv element — so rethrowing this verbatim prints the whole prompt. Keep the exit code.
    const code = (error as { code?: unknown }).code;
    throw new Error(`${options.arm}_session_failed_${typeof code === "number" || typeof code === "string" ? code : "unknown"}`);
  }
  const transcript = newTranscript(dir, before);
  const turns = transcriptUsage(transcript);
  // Steady state starts at turn 2, so a run with fewer turns carries no comparable measurement.
  invariant(turns.length >= 4, `${options.arm}_needs_four_or_more_turns`);
  return summarize(options.arm, options.model, turns);
}

/**
 * Ceiling on how much bigger the gateway arm's FIRST turn may be. Measured 2026-09-15: pointing
 * Claude Code at any custom base URL makes it inline every MCP tool schema (217 tools, 224,077
 * bytes, ~56k tokens here) where the official endpoint sends roughly 4.5k worth. That is a
 * client-side decision, not gateway code, but it is the dominant quota difference on an
 * MCP-heavy machine and an eval that ignored it would call a 2.4x session "at parity".
 */
const FIRST_TURN_INFLATION_MAX = 1.25;

function compare(native: ArmResult, gateway: ArmResult): { checks: { name: string; passed: boolean }[]; passed: boolean } {
  const nativeFirst = native.turns[0]?.inclusive ?? 0;
  const gatewayFirst = gateway.turns[0]?.inclusive ?? 0;
  const checks: { name: string; passed: boolean }[] = [
    {
      name: "gateway_first_turn_not_inflated",
      passed: nativeFirst > 0 && gatewayFirst / nativeFirst <= FIRST_TURN_INFLATION_MAX,
    },
    { name: "native_warmed_read_share_healthy", passed: (native.warmedReadShare ?? 0) >= WARM_READ_SHARE_MIN },
    { name: "gateway_warmed_read_share_healthy", passed: (gateway.warmedReadShare ?? 0) >= WARM_READ_SHARE_MIN },
    {
      name: "gateway_read_share_within_delta_of_native",
      passed: (native.warmedReadShare ?? 0) - (gateway.warmedReadShare ?? 0) <= READ_SHARE_DELTA_MAX,
    },
    { name: "gateway_no_raw_input_blowup", passed: gateway.maxRaw <= Math.max(native.maxRaw * 2, 100_000) },
    {
      name: "gateway_warm_writes_comparable",
      passed: gateway.maxWarmWrite <= Math.max(native.maxWarmWrite * 2, 10_000),
    },
  ];
  return { checks, passed: checks.every(check => check.passed) };
}

async function main(): Promise<void> {
  invariant(process.env.OPENCODEX_PARITY_LIVE === LIVE_OPT_IN, "live_opt_in_missing");
  const promptPath = process.argv[2];
  invariant(promptPath && existsSync(promptPath), "prompt_file_missing");
  const prompt = readFileSync(resolve(promptPath), "utf8").trim();
  invariant(prompt.length > 0, "prompt_empty");
  // Default to the same model on both arms: parity is a claim about the gateway's own overhead.
  // Set this to `combo/waterfall` to measure the failover path instead, where a hop to a
  // different vendor legitimately changes cache behaviour and parity does NOT apply.
  const gatewayModel = process.env.OPENCODEX_PARITY_GATEWAY_MODEL?.trim() || "claude-opus-5";
  const nativeModel = process.env.OPENCODEX_PARITY_NATIVE_MODEL?.trim() || "claude-opus-5";

  const port = await freeLoopbackPort();
  const root = mkdtempSync(join(tmpdir(), "ocx-session-parity-"));
  const opencodexHome = join(root, "opencodex");
  const codexHome = join(root, "codex");
  const claudeHome = join(root, "claude");
  const claudeDesktopHome = join(root, "claude-desktop");
  seedIsolatedHome(opencodexHome, [codexHome, claudeHome, claudeDesktopHome]);

  // Every home the gateway process might write to is redirected into the throwaway root. Startup
  // sync rewrites Codex and Claude settings; pointed at the real dirs it would reconfigure live
  // sessions, which is exactly the 2026-09-01 `ocx start` incident.
  // `src/index.ts` is the library entry and exits 0 without starting anything; the CLI lives in
  // `src/cli/index.ts`.
  const gateway = Bun.spawn(["bun", "src/cli/index.ts", "start", "--port", String(port)], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      // Startup also reconciles `$HOME/.zshrc` (it will DELETE the user's OpenCodex hook block
      // when it believes the hook is not installed) and writes `$HOME/.grok/config.toml`. Neither
      // path honours OPENCODEX_HOME, so HOME itself must point into the throwaway root.
      HOME: root,
      OPENCODEX_HOME: opencodexHome,
      CODEX_HOME: codexHome,
      CLAUDE_CONFIG_DIR: claudeHome,
      OPENCODEX_CLAUDE_DESKTOP_CONFIG_DIR: claudeDesktopHome,
    },
    stdout: "pipe",
    stderr: "pipe",
  });

  let report: Record<string, unknown>;
  try {
    const deadline = Date.now() + GATEWAY_READY_TIMEOUT_MS;
    let ready = false;
    while (Date.now() < deadline) {
      if (await portAcceptsConnections(port)) {
        ready = true;
        break;
      }
      await Bun.sleep(250);
    }
    invariant(ready, "gateway_never_listened");

    const nativeArm = await runSession({ arm: "native", model: nativeModel, prompt });
    const gatewayArm = await runSession({
      arm: "gateway",
      model: gatewayModel,
      prompt,
      baseUrl: `http://127.0.0.1:${port}`,
    });
    const comparison = compare(nativeArm, gatewayArm);
    report = {
      schemaVersion: 1,
      mode: "live",
      // Which configuration these numbers describe. A report without it is unreadable six months
      // from now: the same gateway measures 33,984 or 103,158 on turn 0 depending on this flag.
      toolSearch,
      port,
      passed: comparison.passed,
      native: { ...nativeArm, turns: nativeArm.turns.length },
      gateway: { ...gatewayArm, turns: gatewayArm.turns.length },
      checks: comparison.checks,
    };
  } finally {
    gateway.kill();
    await gateway.exited;
    const closed = !(await portAcceptsConnections(port));
    // The isolated home holds a COPY of the real auth.json. Leaving it behind on every run
    // scatters live credentials through the temp directory, so removal is part of the run, not
    // an optional tidy-up. Only ever removes the directory this run created.
    let stateRemoved = false;
    try {
      rmSync(root, { recursive: true, force: true });
      stateRemoved = !existsSync(root);
    } catch {
      stateRemoved = false;
    }
    report ??= {};
    (report as Record<string, unknown>).gatewayStopped = true;
    (report as Record<string, unknown>).portClosed = closed;
    (report as Record<string, unknown>).isolatedStateRemoved = stateRemoved;
  }

  const reportPath = process.env.OPENCODEX_PARITY_REPORT?.trim();
  const serialized = JSON.stringify(report, null, 2);
  if (reportPath) writeFileSync(resolve(reportPath), serialized);
  console.log(serialized);
  process.exit((report as { passed?: boolean }).passed ? 0 : 1);
}

await main();
