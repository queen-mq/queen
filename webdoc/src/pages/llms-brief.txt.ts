// `/llms-brief.txt` — the whole product in one fetch.
//
// The route is thin on purpose: the collation and the reasoning behind it live
// in `src/lib/llms-brief.ts`, because `pages/llms.txt.ts` imports the same
// builder to advertise this document's real size rather than a number somebody
// remembered to update.
import { buildBrief } from "@/lib/llms-brief";

export const prerender = true;

export async function GET() {
  return new Response(await buildBrief(), {
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
}
