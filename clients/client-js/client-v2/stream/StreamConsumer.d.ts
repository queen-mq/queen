/**
 * StreamConsumer - consumes windows from a stream (legacy stream API,
 * `queen.consumer(streamName, consumerGroup)`).
 */

import type { QueenHttpClient } from '../types.js'
import type { Window } from './Window.js'

export declare class StreamConsumer {
  constructor(httpClient: QueenHttpClient, queen: unknown, streamName: string, consumerGroup: string)
  /** Process windows with the callback, in a loop until `stop()`. */
  process(callback: (window: Window) => unknown): Promise<void>
  /** Stop the processing loop. */
  stop(): void
  /** Poll for a window (blocking call); null when there is none. */
  pollWindow(): Promise<Window | null>
  /** Run the callback for one window, renewing its lease meanwhile. */
  executeCallback(window: Window, callback: (window: Window) => unknown): Promise<void>
  /** Seek to an ISO timestamp. */
  seek(timestamp: string): Promise<unknown>
}
