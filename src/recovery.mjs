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

/**
 * A timed-out invocation remains observable, but cannot launch a second
 * process if its previous dispatch outcome became uncertain.
 * @param {"dispatch"|"reattach"|"collect"|"wait"|"unknown"|"lost"} action
 * @param {boolean} previouslyUnknown
 * @returns {"dispatch"|"reattach"|"collect"|"wait"|"unknown"|"lost"}
 */
export function forbidUncertainRedispatch(action, previouslyUnknown) {
  return previouslyUnknown && action === "dispatch" ? "unknown" : action;
}

/**
 * Keep the active lock while a command is uncertain. Only the same task can
 * re-observe it; a lost workspace or unacknowledged restore blocks everyone.
 * @param {{ active: string | undefined, requested: string, restoreRequired: boolean, reconciliationRequired: boolean }} state
 */
export function observationGate(state) {
  if (state.restoreRequired || state.reconciliationRequired) return "blocked";
  if (state.active && state.active !== state.requested) return "blocked";
  return "observe";
}
