/**
 * @queenmq/streams/operators — re-exports for advanced users who want to
 * build custom operator chains directly. Most code should just use the
 * fluent Stream builder from the package root.
 */

export { MapOperator } from './MapOperator.js'
export { FilterOperator } from './FilterOperator.js'
export { FlatMapOperator } from './FlatMapOperator.js'
export { KeyByOperator } from './KeyByOperator.js'
export { WindowTumblingOperator } from './WindowTumblingOperator.js'
export { WindowSlidingOperator } from './WindowSlidingOperator.js'
export { WindowSessionOperator } from './WindowSessionOperator.js'
export { WindowCronOperator } from './WindowCronOperator.js'
export { ReduceOperator, stateKeyFor, parseStateKey } from './ReduceOperator.js'
export { AggregateOperator, RESERVED_AGGREGATE_FIELDS } from './AggregateOperator.js'
export { SinkOperator } from './SinkOperator.js'
export { ForeachOperator } from './ForeachOperator.js'
