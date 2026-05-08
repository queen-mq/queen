/**
 * configHash — stable hash of an operator chain's structural shape.
 *
 * Used to detect that the user redeployed a stream under the same queryId
 * but with a different operator chain. The server refuses such re-registers
 * unless the user passes `{ reset: true }`.
 *
 * Hash inputs (intentional):
 *   - operator kinds in order
 *   - structural config of each operator (window size, aggregate fields,
 *     sink queue name, ...)
 * Hash inputs (intentionally NOT included):
 *   - user-supplied closures (.map fn, .filter predicate, .reduce fn). We
 *     cannot stably serialise functions, and they often vary across deploys
 *     for trivial reasons (formatting, comments). Hashing only the
 *     structural shape is a safety net for "you reordered the chain or
 *     changed window size", which is what actually corrupts state.
 */

import crypto from 'node:crypto'

export function configHashOf(operators) {
  const parts = operators.map(op => describe(op))
  const json = JSON.stringify(parts)
  return crypto.createHash('sha256').update(json).digest('hex')
}

function describe(op) {
  // Each operator may expose a `.config` object summarising its structural
  // shape. Fall back to just the kind name.
  const cfg = op && typeof op.config === 'object' && op.config !== null ? op.config : {}
  return { kind: op.kind, ...cfg }
}
