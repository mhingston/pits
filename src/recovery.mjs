/**
 * Pure, conservative decision table. A missing directory is proof of no launch
 * ONLY on the same container boot, and only if PITS never deletes reservations.
 * Container-local reservations must never be restored from workspace backups.
 *
 * @param {{ bootId: string } | undefined} intent
 * @param {string} bootId
 * @param {"missing"|"starting"|"running"|"exited"|"lost"} observation
 * @returns {"dispatch"|"reattach"|"collect"|"wait"|"unknown"|"lost"}
 */
export function decide(intent, bootId, observation) {
  if (!intent) return "dispatch";
  if (intent.bootId !== bootId) return "lost";
  switch (observation) {
    case "missing": return "dispatch";
    case "running": return "reattach";
    case "exited": return "collect";
    case "starting": return "wait";
    case "lost": return "unknown";
    default: return "unknown";
  }
}
