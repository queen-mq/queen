<?php

namespace Queen\Support;

/**
 * The `where` of a filtered consumer group (QueueBuilder::where()) and of a
 * reading queue (config(['from' => ..., 'where' => ...])): conditions on one
 * message, ANDed, that the broker checks as the message is pushed.
 *
 * Two forms say the same thing, and the broker takes either:
 *
 *  - a list of conditions, each Where::condition($field, $op, $value), or
 *    [$field, $op, $value], or ['field' => ..., 'op' => ..., 'value' => ...];
 *  - one string: "data.kind in ('a', 'b') and headers.type = 'x' and data.total > 100".
 *
 * Fields: queue, partition, transactionId, data, data.<path> (16 deep) and
 * headers.<name>. Ops: = != < <= > >= in exists prefix !prefix; `in` takes a
 * list (64 values by default), prefix a string, a comparison a number or a
 * string. At most 16 conditions by default, and no OR: `in` covers most of
 * it. A field the message does not have holds for no op, `!=` included. The
 * broker checks the fields, the limits and the string (400); here only the
 * shape is, so a mistake is named before anything is sent.
 */
final class Where
{
    public const OPS = ['=', '==', '!=', '<', '<=', '>', '>=', 'in', 'exists', 'prefix', '!prefix'];

    /**
     * One condition, checked: ['field' => ..., 'op' => ..., 'value' => ...],
     * without a value for `exists`.
     *
     *     Where::condition('data.kind', 'in', ['order.paid', 'order.refunded'])
     *     Where::condition('headers.type', '=', 'order.paid')
     *     Where::condition('data.total', '>', 100)
     *     Where::condition('data.vip', 'exists')
     *
     * @return array{field: string, op: string, value?: mixed}
     */
    public static function condition(string $field, string $op, mixed $value = null): array
    {
        if ($field === '') {
            throw new \InvalidArgumentException('where: a field is a non-empty string');
        }
        if (!in_array($op, self::OPS, true)) {
            throw new \InvalidArgumentException("where: unknown op [{$op}] (= != < <= > >= in exists prefix !prefix)");
        }
        if ($op === 'in' && (!is_array($value) || !array_is_list($value))) {
            throw new \InvalidArgumentException("where: in on {$field} needs a list of values");
        }
        if (($op === 'prefix' || $op === '!prefix') && !is_string($value)) {
            throw new \InvalidArgumentException("where: {$op} on {$field} needs a string");
        }
        $condition = ['field' => $field, 'op' => $op];
        if ($op !== 'exists') {
            $condition['value'] = $value;
        }

        return $condition;
    }

    /**
     * `where` as a caller wrote it, in the form the broker reads: one string,
     * kept as it is; or one condition, or a list of them, each checked.
     *
     * @return string|list<array{field: string, op: string, value?: mixed}>
     */
    public static function of(string|array $where): string|array
    {
        if (is_string($where)) {
            if (trim($where) === '') {
                throw new \InvalidArgumentException('where: the string holds no condition');
            }
            return $where;
        }
        if ($where === []) {
            throw new \InvalidArgumentException('where: no condition');
        }
        // One condition on its own: ['data.kind', '=', 'x'] or ['field' => ...].
        if (isset($where['field']) || (array_is_list($where) && is_string($where[0]))) {
            $where = [$where];
        }
        if (!array_is_list($where)) {
            throw new \InvalidArgumentException('where: a list of conditions, or one string');
        }

        return array_map(static fn (mixed $condition): array => self::checked($condition), $where);
    }

    /** A pop's `where` parameter: the string, or the list as its JSON text. */
    public static function param(string|array $where): string
    {
        return is_string($where) ? $where : json_encode($where, JSON_THROW_ON_ERROR);
    }

    private static function checked(mixed $condition): array
    {
        if (is_array($condition) && isset($condition['field'])) {
            $unknown = array_diff(array_keys($condition), ['field', 'op', 'value']);
            if ($unknown !== []) {
                throw new \InvalidArgumentException('where: unknown key [' . reset($unknown) . '] in a condition');
            }
            [$field, $op, $value] = [$condition['field'], $condition['op'] ?? null, $condition['value'] ?? null];
        } elseif (is_array($condition) && array_is_list($condition) && in_array(count($condition), [2, 3], true)) {
            [$field, $op, $value] = [$condition[0], $condition[1], $condition[2] ?? null];
        } else {
            throw new \InvalidArgumentException('where: a condition is [field, op, value] or [\'field\' => ..., \'op\' => ..., \'value\' => ...]');
        }
        if (!is_string($field) || !is_string($op)) {
            throw new \InvalidArgumentException('where: a condition names its field and its op as strings');
        }

        return self::condition($field, $op, $value);
    }
}
