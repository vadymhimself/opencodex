import { classifyError, upstreamErrorMessageFromPayload } from "../lib/errors";
import { readQuotaResetAt } from "../providers/key-failover";

/**
 * Why an upstream answer may move a request to the next Anthropic pool account.
 *
 * - `"429"`: a rate or quota limit, the original trigger.
 * - `"quota-400"`: Anthropic answers an account whose plan AND extra usage are spent with
 *   HTTP 400 ("You're out of extra usage", "Third-party apps now draw from your extra usage"),
 *   not 429. The account is spent; another pool account may not be.
 */
export type AnthropicPoolRotationTrigger = "429" | "quota-400";

/**
 * Whether a 400 body says the account is out of quota. Reuses the shared classifier so the
 * wording lives in one place (src/lib/errors.ts); every other 400 (a malformed request) is a
 * request-shape failure that would fail the same way on any account and must not rotate.
 */
export function isQuotaExhaustedBadRequest(status: number, body: string | null | undefined): boolean {
  if (status !== 400 || !body?.trim()) return false;
  let message = body;
  try {
    message = upstreamErrorMessageFromPayload(JSON.parse(body)) ?? body;
  } catch { /* not JSON: classify the raw text */ }
  return classifyError(400, "invalid_request_error", message).code === "insufficient_quota";
}

/** The pool-rotation trigger for an upstream status and (for a 400) its body, or null. */
export function isPoolRotatableStatus(
  status: number,
  body?: string | null,
): AnthropicPoolRotationTrigger | null {
  if (status === 429) return "429";
  return isQuotaExhaustedBadRequest(status, body) ? "quota-400" : null;
}

/**
 * Decide whether `response` should rotate the Anthropic pool. Only a 400 body is read: a bounded
 * prefix through the same peek the key-pool 429 path uses, and the returned response replays it,
 * so a caller that does not rotate still forwards the error unchanged. Any other status is
 * returned untouched. Throws only when `signal` aborts (client cancelled).
 */
export async function peekAnthropicPoolRotation(
  response: Response,
  signal?: AbortSignal,
): Promise<{ response: Response; trigger: AnthropicPoolRotationTrigger | null }> {
  if (response.status !== 400) return { response, trigger: isPoolRotatableStatus(response.status) };
  const peeked = await readQuotaResetAt(response, { signal });
  return { response: peeked.response, trigger: isPoolRotatableStatus(400, peeked.text) };
}
