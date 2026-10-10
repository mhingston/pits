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

/**
 * A failure to query process state is ambiguous on the same boot. A changed
 * boot proves that a prior dispatch can no longer be observed or replayed.
 * @param {string} intentBootId
 * @param {string | undefined} observedBootId
 * @returns {"unknown"|"lost"}
 */
export function classifyObservationFailure(intentBootId, observedBootId) {
  return observedBootId && observedBootId !== intentBootId ? "lost" : "unknown";
}

/**
 * A restore permanently invalidates command receipts whose effects were made
 * after the restored checkpoint. Preserve prior ranges across later restores
 * so a lost receipt cannot become successful again after another checkpoint.
 * @param {readonly { afterSequence: number, throughSequence: number }[]} ranges
 * @param {number} afterSequence
 * @param {number} throughSequence
 * @returns {{ afterSequence: number, throughSequence: number }[]}
 */
export function addLostSequenceRange(ranges, afterSequence, throughSequence) {
  if (throughSequence <= afterSequence) return [...ranges];
  const sorted = [...ranges, { afterSequence, throughSequence }]
    .sort((left, right) => left.afterSequence - right.afterSequence);
  const merged = [];
  for (const range of sorted) {
    const previous = merged.at(-1);
    if (previous && range.afterSequence <= previous.throughSequence) {
      previous.throughSequence = Math.max(previous.throughSequence, range.throughSequence);
    } else {
      merged.push({ ...range });
    }
  }
  return merged;
}

/** @param {number | undefined} sequence @param {readonly { afterSequence: number, throughSequence: number }[]} ranges */
export function receiptWasInvalidated(sequence, ranges) {
  return sequence !== undefined && ranges.some(range =>
    sequence > range.afterSequence && sequence <= range.throughSequence
  );
}
