<?php

namespace Queen\Laravel\Monitoring;

use Illuminate\Database\Eloquent\Collection as EloquentCollection;
use Illuminate\Database\Eloquent\Model;

/**
 * The tags of a job, as Horizon computes them: the job's own `tags()` when
 * it has one, otherwise one `Model:key` tag per Eloquent model it carries.
 * Queued listeners, mailables, notifications and broadcasts are unwrapped to
 * the object that carries the models.
 *
 * Computed when the job is pushed and stored in its payload under `tags`, so
 * a worker reads them without unserializing the command. A model is always
 * tagged by its key, even when it has a `tags()` relation, and a `tags()` that
 * throws or returns no list is ignored: tagging never fails a dispatch.
 */
final class JobTags
{
    public const MAX_TAGS = 20;

    public const MAX_TAG_BYTES = 128;

    private const MAX_MODELS_PER_COLLECTION = 5;

    /** @return list<string> */
    public static function for(mixed $job): array
    {
        if (!is_object($job)) {
            return [];
        }
        try {
            $tags = [];
            foreach (self::targets($job) as $target) {
                $tags = [...$tags, ...(!$target instanceof Model && method_exists($target, 'tags')
                    ? self::explicit($target)
                    : self::models($target))];
            }

            return self::normalize($tags);
        } catch (\Throwable) {
            return [];
        }
    }

    /** @return array<mixed> */
    private static function explicit(object $target): array
    {
        $tags = $target->tags();

        return match (true) {
            is_array($tags) => $tags,
            $tags instanceof \Traversable => iterator_to_array($tags, false),
            default => [],
        };
    }

    /**
     * @param mixed $tags anything a payload may carry
     * @return list<string>
     */
    public static function normalize(mixed $tags): array
    {
        $normalized = [];
        foreach (is_array($tags) ? $tags : [] as $tag) {
            if (!is_string($tag) && !is_int($tag)) {
                continue;
            }
            $tag = trim((string) $tag);
            if ($tag === '' || strlen($tag) > self::MAX_TAG_BYTES || preg_match('/[\x00-\x1F\x7F]/', $tag) === 1) {
                continue;
            }
            $normalized[$tag] = true;
            if (count($normalized) >= self::MAX_TAGS) {
                break;
            }
        }

        return array_map('strval', array_keys($normalized));
    }

    /** @return list<object> */
    private static function targets(object $job): array
    {
        return match (true) {
            $job instanceof \Illuminate\Events\CallQueuedListener => array_values(array_filter($job->data, 'is_object')),
            $job instanceof \Illuminate\Mail\SendQueuedMailable => [$job->mailable],
            $job instanceof \Illuminate\Notifications\SendQueuedNotifications => [$job->notification, ...array_values(array_filter(
                $job->notifiables instanceof \Traversable ? iterator_to_array($job->notifiables) : (array) $job->notifiables,
                'is_object',
            ))],
            $job instanceof \Illuminate\Broadcasting\BroadcastEvent => [$job->event],
            default => [$job],
        };
    }

    /** @return list<string> */
    private static function models(object $target): array
    {
        if ($target instanceof Model) {
            return [self::modelTag($target)];
        }
        $tags = [];
        foreach ((new \ReflectionObject($target))->getProperties() as $property) {
            if ($property->isStatic() || !$property->isInitialized($target)) {
                continue;
            }
            $value = $property->getValue($target);
            if ($value instanceof Model) {
                $tags[] = self::modelTag($value);
            } elseif ($value instanceof EloquentCollection) {
                foreach ($value->take(self::MAX_MODELS_PER_COLLECTION) as $model) {
                    if ($model instanceof Model) {
                        $tags[] = self::modelTag($model);
                    }
                }
            }
        }

        return $tags;
    }

    private static function modelTag(Model $model): string
    {
        return $model::class . ':' . $model->getKey();
    }
}
