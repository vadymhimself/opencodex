import { expect, test } from "bun:test";
import { refuseImageOnPin, refuseMediaBridgeOnPin, refuseWebSearchOnPin } from "../../src/server/responses/pinned-helper-refusal";

// AGE-2004: each refused helper case says what was refused, that the pin blocks helpers, and the one next step.
const cases = [
  ["image on a text-only model", refuseImageOnPin, "it contains an image", "vision helper", "resend without the image"],
  ["web_search on a non-Claude pin", refuseWebSearchOnPin, "it asks for the web_search tool", "search helper", "resend without the web_search tool"],
  ["image or video bridge", refuseMediaBridgeOnPin, "it needs the image or video bridge", "image or video bridge", "resend without image or video generation"],
] as const;

for (const [name, refuse, what, helper, cta] of cases) {
  test(`pinned refusal: ${name}`, async () => {
    const res = refuse();
    expect(res.status).toBe(400);
    const text = (await res.json()).error.message as string;
    expect(text).toContain("Request refused, nothing was sent");
    expect(text).toContain(what);
    expect(text).toContain(`cannot use helper models, so no ${helper} was allowed to run`);
    expect(text).toContain(cta);
    expect(text).toContain("ask the account owner to turn on Fall back to other AI accounts");
  });
}
