import { formatErrorResponse } from "../../bridge";

// AGE-2004: what an agent sees when its model pin blocks a helper model. Names the refused case, why, and the one next step.
const NOW = "Now: resend without it, or ask the account owner to turn on Fall back to other AI accounts.";
const refusal = (what: string, helper: string) => formatErrorResponse(400, "invalid_request_error",
  `Request refused, nothing was sent: ${what}. This agent's model pin cannot use helper models, so no ${helper} was allowed to run. ${NOW}`);

export const refuseImageOnPin = () => refusal("it contains an image and the pinned model cannot read images (resend without the image, describe it in words)", "vision helper");
export const refuseWebSearchOnPin = () => refusal("it asks for the web_search tool and the pin is not a Claude account (resend without the web_search tool)", "search helper");
export const refuseMediaBridgeOnPin = () => refusal("it needs the image or video bridge (image_gen/video_gen) (resend without image or video generation)", "image or video bridge");
