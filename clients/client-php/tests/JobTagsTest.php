<?php

namespace Queen\Tests;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Model;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Monitoring\JobTags;

final class JobTagsTest extends TestCase
{
    public function testAJobsOwnTagsWin(): void
    {
        $job = new class () {
            public $user;

            public function __construct()
            {
                $this->user = TagsTestUser::withKey(7);
            }

            public function tags(): array
            {
                return ['billing', 'customer:7', 'billing'];
            }
        };

        $this->assertSame(['billing', 'customer:7'], JobTags::for($job));
    }

    public function testAModelWithATagsRelationIsStillTaggedByItsKey(): void
    {
        $job = new class () {
            public $post;

            public function __construct()
            {
                $this->post = TagsTestPost::withKey(3);
            }
        };

        $this->assertSame([TagsTestPost::class . ':3'], JobTags::for(new \Illuminate\Events\CallQueuedListener('Listener', 'handle', [TagsTestPost::withKey(3)])));
        $this->assertSame([TagsTestPost::class . ':3'], JobTags::for($job));
    }

    public function testCollectionTagsCountAndABrokenTagsMethodNeverFailsTheDispatch(): void
    {
        $collection = new class () {
            public function tags(): \Illuminate\Support\Collection
            {
                return collect(['reports', 'team:4']);
            }
        };
        $throwing = new class () {
            public function tags(): array
            {
                throw new \RuntimeException('no tags');
            }
        };
        $scalar = new class () {
            public function tags(): string
            {
                return 'not a list';
            }
        };

        $this->assertSame(['reports', 'team:4'], JobTags::for($collection));
        $this->assertSame([], JobTags::for($throwing));
        $this->assertSame([], JobTags::for($scalar));
    }

    public function testEloquentModelsAreTaggedByClassAndKey(): void
    {
        $job = new class () {
            public $user;
            protected $orders;
            private $note = 'not a model';

            public function __construct()
            {
                $this->user = TagsTestUser::withKey(42);
                $this->orders = new Collection([TagsTestUser::withKey(1), TagsTestUser::withKey(2)]);
            }
        };

        $this->assertSame([
            TagsTestUser::class . ':42',
            TagsTestUser::class . ':1',
            TagsTestUser::class . ':2',
        ], JobTags::for($job));
    }

    public function testAQueuedMailableIsUnwrapped(): void
    {
        $mailable = new class () extends \Illuminate\Mail\Mailable {
            public $user;
        };
        $mailable->user = TagsTestUser::withKey(9);

        $this->assertSame([TagsTestUser::class . ':9'], JobTags::for(new \Illuminate\Mail\SendQueuedMailable($mailable)));
    }

    public function testTagsAreBoundedAndCleaned(): void
    {
        $this->assertSame(['ok', '12'], JobTags::normalize(['ok', '', "bad\ntag", str_repeat('x', 129), 12, ['nested'], ' ok ']));
        $this->assertCount(JobTags::MAX_TAGS, JobTags::normalize(array_map(fn (int $i): string => "tag-{$i}", range(1, 50))));
        $this->assertSame([], JobTags::for('not a job'));
    }
}

final class TagsTestPost extends Model
{
    public static function withKey(int $key): self
    {
        $post = new self();
        $post->id = $key;

        return $post;
    }

    /** A relation, as spatie/laravel-tags adds: not the job's tag list. */
    public function tags(): \Illuminate\Database\Eloquent\Relations\HasMany
    {
        return $this->hasMany(TagsTestUser::class);
    }
}

final class TagsTestUser extends Model
{
    public static function withKey(int $key): self
    {
        $user = new self();
        $user->id = $key;

        return $user;
    }
}
