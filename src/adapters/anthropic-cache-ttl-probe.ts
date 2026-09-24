/**
 * Test seam for the prompt-cache TTL scan.
 *
 * The scan itself is private to the adapter because nothing outside it should choose a TTL, but
 * it reads the CALLER's raw body on the replay lane and therefore has to survive every shape the
 * Anthropic wire allows. That robustness is worth pinning, and pinning it needs a way in.
 */
export { effectivePromptCacheTtlMs as effectivePromptCacheTtlMsForTest } from "./anthropic";
