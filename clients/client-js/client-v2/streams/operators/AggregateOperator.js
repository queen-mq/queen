/**
 * AggregateOperator — sugar over reduce. Each named extractor produces one
 * field of the per-window aggregate value.
 *
 * Supported extractor names:
 *   count: () => 1                         // scalar count contribution
 *   sum:   m => m.data.amount              // running sum
 *   min:   m => m.data.value               // running min (ignores nulls)
 *   max:   m => m.data.value               // running max (ignores nulls)
 *   avg:   m => m.data.value               // running average (sum + count internally)
 *
 * Output shape per window:
 *   { count?: number, sum?: number, min?: number, max?: number, avg?: number, ... }
 *
 * Custom keys (any name not in the list above) are also supported and
 * treated as `sum`-like (numeric running total). Use plain `.reduce(...)`
 * if you need non-additive aggregates.
 */

import { ReduceOperator } from './ReduceOperator.js'

const RESERVED = new Set(['count', 'sum', 'min', 'max', 'avg'])

export class AggregateOperator extends ReduceOperator {
  constructor(extractors) {
    if (!extractors || typeof extractors !== 'object') {
      throw new Error('aggregate requires an object of named extractors')
    }
    const fields = Object.keys(extractors).map(name => {
      const fn = extractors[name]
      if (typeof fn !== 'function') {
        throw new Error(`aggregate.${name} must be a function`)
      }
      return { name, fn }
    })
    if (fields.length === 0) {
      throw new Error('aggregate requires at least one extractor')
    }

    const initial = {}
    for (const { name } of fields) {
      if (name === 'min') initial[name] = null
      else if (name === 'max') initial[name] = null
      else if (name === 'avg') { initial.__avg_sum = 0; initial.__avg_count = 0; initial.avg = 0 }
      else initial[name] = 0
    }

    const fn = async (acc, msg) => {
      const next = { ...acc }
      for (const { name, fn: ex } of fields) {
        const v = await ex(msg)
        if (name === 'count') {
          next.count = (next.count || 0) + (typeof v === 'number' ? v : 1)
        } else if (name === 'sum') {
          next.sum = (next.sum || 0) + (typeof v === 'number' ? v : 0)
        } else if (name === 'min') {
          if (typeof v === 'number') {
            next.min = next.min === null || next.min === undefined ? v : Math.min(next.min, v)
          }
        } else if (name === 'max') {
          if (typeof v === 'number') {
            next.max = next.max === null || next.max === undefined ? v : Math.max(next.max, v)
          }
        } else if (name === 'avg') {
          if (typeof v === 'number') {
            next.__avg_sum = (next.__avg_sum || 0) + v
            next.__avg_count = (next.__avg_count || 0) + 1
            next.avg = next.__avg_count > 0 ? next.__avg_sum / next.__avg_count : 0
          }
        } else {
          // Custom user-named field; treat as numeric running total.
          next[name] = (next[name] || 0) + (typeof v === 'number' ? v : 0)
        }
      }
      return next
    }

    super(fn, initial)
    this.kind = 'aggregate'
    this.fields = fields.map(f => f.name)
    // For configHash: expose the extractor field names + presence of reserved keys.
    this.config = { fields: fields.map(f => f.name) }
  }
}

// Re-export the constant in case downstream wants the supported list.
export const RESERVED_AGGREGATE_FIELDS = Array.from(RESERVED)
