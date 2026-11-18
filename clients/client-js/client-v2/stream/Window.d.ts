/**
 * Window - a time window of messages with utility methods (legacy stream API,
 * `queen.consumer(...)`). The raw window's own fields are copied onto it.
 */

import type { JsonObject } from '../types.js'

/** `window.aggregate()` configuration: each list names dot paths into the messages. */
export interface WindowAggregateConfig {
  count?: boolean | undefined
  sum?: string[] | undefined
  avg?: string[] | undefined
  min?: string[] | undefined
  max?: string[] | undefined
}

/** `window.aggregate()` results. */
export interface WindowAggregateResult {
  count?: number
  sum?: Record<string, number>
  avg?: Record<string, number>
  min?: Record<string, number | null>
  max?: Record<string, number | null>
}

export declare class Window {
  constructor(rawWindow: JsonObject)
  /** The original messages, frozen. */
  readonly allMessages: readonly any[]
  /** The working set, narrowed by `filter()`. */
  messages: any[]
  /** The raw window's other fields. */
  [key: string]: unknown
  /** Filter the working set. */
  filter(filterFn: (msg: any, index: number, all: any[]) => boolean): this
  /** Group the working set by a dot path (e.g. 'data.userId'). */
  groupBy(keyPath: string): Record<string, any[]>
  /** Aggregate the working set. */
  aggregate(config?: WindowAggregateConfig): WindowAggregateResult
  /** Reset the working set to the original messages. */
  reset(): this
  /** Messages in the working set. */
  size(): number
  /** Original messages. */
  originalSize(): number
}
