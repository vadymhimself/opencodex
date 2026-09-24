/**
 * Turn a cached quota reading into a sentence a human can act on.
 *
 * Upstream answers a spent model-family window with the same generic `rate_limit_error` it uses
 * for ordinary request-rate throttling -- "this request would exceed your account's rate limit,
 * please try again later". That advice is wrong for an exhausted window: trying again later does
 * not help until the window rolls, and the operator's real options (a different model, a
 * different account) are invisible in it.
 *
 * This gateway already probes and caches the per-window percentages, so it can name the window
 * that is actually spent. Nothing here infers or predicts: it reports a cached reading, says so,
 * and stays silent when the cache has nothing to add.
 */
import { getCachedProviderAccountQuota } from "./quota/account-cache";
import type { ProviderQuota, ProviderQuotaWindow } from "./quota-types";

/** A window counts as spent at 100% with a reset that has not yet passed. */
function windowSpent(percent: number | undefined, resetAt: number | undefined, now: number): boolean {
  if (typeof percent !== "number" || !Number.isFinite(percent) || percent < 100) return false;
  return typeof resetAt !== "number" || !Number.isFinite(resetAt) || resetAt > now;
}

function resetPhrase(resetAt: number | undefined, now: number): string {
  if (typeof resetAt !== "number" || !Number.isFinite(resetAt) || resetAt <= now) return "";
  const minutes = Math.round((resetAt - now) / 60_000);
  if (minutes < 60) return `, resets in ${Math.max(1, minutes)}m`;
  const hours = minutes / 60;
  // A trailing ".0" reads as spurious precision on a cached figure, so whole hours stay whole.
  if (hours < 48) {
    const rounded = hours < 10 ? Math.round(hours * 10) / 10 : Math.round(hours);
    return `, resets in ${rounded}h`;
  }
  return `, resets in ${Math.round(hours / 24)}d`;
}

/**
 * Only a window the PRODUCER proved model-scoped is reported against a model name; everything
 * else is provider-wide. Same rule the routing gate uses, and for the same reason: a label is
 * upstream text and cannot be trusted to mean what it looks like.
 */
function windowLabel(window: ProviderQuotaWindow): string {
  return window.scope === "model" ? `${window.label} window` : `${window.label}`;
}

/**
 * A short clause naming the spent window, or undefined when the cache cannot explain the refusal.
 *
 * Deliberately returns undefined rather than guessing: a 429 with no exhausted window in cache is
 * ordinary rate limiting, and saying otherwise would send the operator after a quota problem that
 * does not exist.
 */
export function explainQuotaRefusal(
  quota: ProviderQuota | null,
  model: string | undefined,
  now = Date.now(),
): string | undefined {
  if (!quota) return undefined;

  const spent: string[] = [];
  // A model-scoped window is only evidence about a model whose id carries that family.
  for (const window of quota.customWindows ?? []) {
    if (!windowSpent(window.percent, window.resetAt, now)) continue;
    if (window.scope === "model" && model !== undefined
      && !model.toLowerCase().includes(window.label.trim().toLowerCase())) continue;
    spent.push(`${windowLabel(window)} at 100%${resetPhrase(window.resetAt, now)}`);
  }
  if (windowSpent(quota.fiveHourPercent, quota.fiveHourResetAt, now)) {
    spent.push(`5-hour window at 100%${resetPhrase(quota.fiveHourResetAt, now)}`);
  }
  if (windowSpent(quota.weeklyPercent, quota.weeklyResetAt, now)) {
    spent.push(`weekly window at 100%${resetPhrase(quota.weeklyResetAt, now)}`);
  }
  if (spent.length === 0) return undefined;
  return `${spent.join("; ")} (cached quota reading)`;
}

/**
 * The same explanation for an OAuth pool account, which is where a per-model window actually
 * lives: the routing cache is key-auth only, so an Anthropic pool refusal has to be read from
 * the account's own reading or it reads as nothing at all.
 */
export function explainAccountQuotaRefusal(
  providerName: string,
  accountId: string | undefined,
  model: string | undefined,
  now = Date.now(),
): string | undefined {
  if (!accountId) return undefined;
  const note = explainQuotaRefusal(getCachedProviderAccountQuota(providerName, accountId), model, now);
  return note ? `${providerName}: ${note}` : undefined;
}

/**
 * Whether EVERY account in the pool has this model's window spent, and the soonest one rolls.
 *
 * This is the fact an operator actually needs on a pool refusal. "One account is spent" is not
 * actionable -- the pool should have rotated. "All of them are spent" says the model is
 * unavailable until a stated time, and that the answer is a different model or a new account
 * rather than waiting a minute and retrying.
 *
 * Silent unless the reading is complete: an account the cache cannot speak for might have
 * headroom, and claiming a pool-wide outage on partial evidence is worse than saying nothing.
 */
export function explainPoolQuotaRefusal(
  providerName: string,
  accountIds: readonly string[],
  model: string | undefined,
  now = Date.now(),
): string | undefined {
  if (accountIds.length === 0) return undefined;
  const notes: string[] = [];
  for (const accountId of accountIds) {
    const quota = getCachedProviderAccountQuota(providerName, accountId);
    if (!quota) return undefined;
    const note = explainQuotaRefusal(quota, model, now);
    if (!note) return undefined;
    notes.push(note);
  }
  const plural = accountIds.length === 1 ? "account" : `all ${accountIds.length} accounts`;
  return `${providerName}: ${model ?? "this model"} is exhausted on ${plural} — ${notes[0]}`;
}
