<?php

/**
 * What `queen.job_metrics` costs a Laravel worker, per job.
 *
 *   php benchmarks/job-metrics.php [--jobs=20000] [--runs=5] [--classes=1] [--url=http://127.0.0.1:6632]
 *
 * Each sample is a fresh PHP process that boots a Laravel application
 * (Orchestra Testbench, as the tests do) with the Queen provider and runs
 * Laravel's own `queue:work queen --quiet --stop-when-empty`, with the other
 * arguments a supervisor gives a pool, over --jobs no-op jobs. Three modes,
 * in a rotated order on every run:
 *
 *   off       queen.job_metrics.enabled false: no job-event listener at all
 *   metrics   job metrics on, monitored tags off
 *   defaults  job metrics and tags on, as config/queen.php ships (the jobs
 *             carry no tag, so tags cost only their listeners)
 *
 * Without --url the jobs come from the in-process fake broker of the tests
 * (tests/Support/MetricsBroker.php: one pop and one ACK per job, no network),
 * which also counts every request and the size of every key/value write.
 * With --url they are pushed to a real broker first, on a queue of their
 * own, outside the measurement, and the worker pops them over HTTP; the
 * sample then reads its metrics back through the dashboard's reader, and
 * times key/value writes of the size a worker makes.
 *
 * Reported per mode: the median CPU time (user + system, getrusage) and wall
 * time per job; the difference to `off` is the cost of the metrics. Writes
 * depend on time, not on jobs: one after a worker's first job, then at most
 * one every ten seconds while it works, one when a five-minute bucket
 * closes, and one when it stops. --classes spreads the jobs over that many
 * class names (fake broker only), which sets the size of a write.
 *
 * Then the recorder alone: JobMetricsRecorder::start() and finish() in a
 * loop, with its writes going to the fake broker, in nanoseconds per job.
 *
 * Needs the dev dependencies (composer install). Prints a table, then the
 * whole result as JSON.
 */

use Illuminate\Contracts\Console\Kernel;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Contracts\Queue\ShouldQueue;
use Orchestra\Testbench\Foundation\Application;
use Queen\Laravel\Dashboard\JobMetricsReader;
use Queen\Laravel\Monitoring\JobMetricsRecorder;
use Queen\Laravel\QueenServiceProvider;
use Queen\Queen;
use Queen\Tests\Support\MetricsBroker;
use Symfony\Component\Console\Input\ArgvInput;
use Symfony\Component\Console\Output\BufferedOutput;

require dirname(__DIR__) . '/vendor/autoload.php';

final class BenchmarkNoOpJob implements ShouldQueue
{
    public function handle(): void
    {
    }
}

const MODES = ['off', 'metrics', 'defaults'];

$options = getopt('', ['jobs::', 'runs::', 'classes::', 'url::', 'worker', 'mode::']);
$jobs = max(1, (int) ($options['jobs'] ?? 20000));
$runs = max(1, (int) ($options['runs'] ?? 5));
$classes = max(1, min(JobMetricsRecorder::MAX_CLASSES, (int) ($options['classes'] ?? 1)));
$url = isset($options['url']) ? rtrim((string) $options['url'], '/') : null;
if ($url !== null) {
    // A real broker serves the class of the job it was given.
    $classes = 1;
}

if (isset($options['worker'])) {
    echo json_encode(worker((string) ($options['mode'] ?? 'off'), $jobs, $classes, $url)), "\n";
    exit(0);
}

$samples = array_fill_keys(MODES, []);
for ($run = 0; $run < $runs; ++$run) {
    // Rotated, so that no mode always runs first or last.
    $order = [...array_slice(MODES, $run % 3), ...array_slice(MODES, 0, $run % 3)];
    foreach ($order as $mode) {
        $samples[$mode][] = sample($mode, $jobs, $classes, $url);
        fwrite(STDERR, '.');
    }
}
fwrite(STDERR, "\n");
$recorder = recorderAlone(200_000, $classes);

$summary = [];
foreach (MODES as $mode) {
    $perJob = fn (array $s, string $field): float => $s[$field] / $s['jobs'] * 1e6;
    $last = end($samples[$mode]);
    $summary[$mode] = [
        'cpu_us_per_job' => round(median(array_map(fn (array $s): float => $perJob($s, 'cpu_s'), $samples[$mode])), 2),
        'wall_us_per_job' => round(median(array_map(fn (array $s): float => $perJob($s, 'wall_s'), $samples[$mode])), 2),
        'cpu_us_per_job_runs' => array_map(fn (array $s): float => round($perJob($s, 'cpu_s'), 2), $samples[$mode]),
        'wall_us_per_job_runs' => array_map(fn (array $s): float => round($perJob($s, 'wall_s'), 2), $samples[$mode]),
        ...array_diff_key($last, array_flip(['mode', 'jobs', 'cpu_s', 'wall_s'])),
    ];
}

printf(
    "%d jobs per sample, %d run(s), %d class name(s), %s; PHP %s on %s %s\n\n",
    $jobs, $runs, $classes, $url === null ? 'in-process fake broker' : "broker {$url}", PHP_VERSION, php_uname('s'), php_uname('m'),
);
printf("%-9s %12s %14s %13s %14s %10s %16s\n", 'mode', 'CPU us/job', 'delta vs off', 'wall us/job', 'delta vs off', 'KV writes', 'bytes per write');
foreach ($summary as $mode => $row) {
    printf(
        "%-9s %12.2f %14s %13.2f %14s %10s %16s\n",
        $mode,
        $row['cpu_us_per_job'],
        $mode === 'off' ? '-' : sprintf('%+.2f', $row['cpu_us_per_job'] - $summary['off']['cpu_us_per_job']),
        $row['wall_us_per_job'],
        $mode === 'off' ? '-' : sprintf('%+.2f', $row['wall_us_per_job'] - $summary['off']['wall_us_per_job']),
        $row['kv_writes'] ?? 'n/a',
        ($row['kv_write_bytes'] ?? []) === [] ? '-' : implode('/', array_unique($row['kv_write_bytes'])),
    );
}
if ($url !== null) {
    $read = $summary['metrics']['read_back'];
    printf(
        "\nread back from the broker (metrics mode): %d processed, average %s ms, max %s ms\n",
        $read['processed'], $read['average_ms'] ?? '-', $read['max_ms'] ?? '-',
    );
    printf("a worker's key/value write on this broker: median %.2f ms, p95 %.2f ms\n", $summary['metrics']['write_ms']['median'], $summary['metrics']['write_ms']['p95']);
}
printf("\nrecorder alone: %.0f ns per job (start + finish), %d write(s) over %d jobs\n", $recorder['ns_per_job'], $recorder['writes'], $recorder['jobs']);
echo json_encode([
    'jobs' => $jobs, 'runs' => $runs, 'classes' => $classes, 'broker' => $url ?? 'in-process',
    'php' => PHP_VERSION, 'os' => php_uname('s') . ' ' . php_uname('r') . ' ' . php_uname('m'),
    'modes' => $summary, 'recorder_alone' => $recorder,
], JSON_PRETTY_PRINT), "\n";

/** One sample in a fresh process. */
function sample(string $mode, int $jobs, int $classes, ?string $url): array
{
    $command = [PHP_BINARY, __FILE__, '--worker', "--mode={$mode}", "--jobs={$jobs}", "--classes={$classes}"];
    if ($url !== null) {
        $command[] = "--url={$url}";
    }
    $process = proc_open($command, [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
    $stdout = stream_get_contents($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[1]);
    fclose($pipes[2]);
    $code = proc_close($process);
    $result = json_decode((string) strrchr("\n" . trim((string) $stdout), "\n"), true);
    if ($code !== 0 || !is_array($result)) {
        fwrite(STDERR, "The {$mode} sample failed ({$code}): {$stderr}{$stdout}\n");
        exit(1);
    }

    return $result;
}

function worker(string $mode, int $jobs, int $classes, ?string $url): array
{
    $broker = null;
    $queue = 'default';
    $namespace = 'queen-metrics';
    if ($url === null) {
        $deliveries = [];
        for ($i = 0; $i < $jobs; ++$i) {
            $deliveries[] = MetricsBroker::delivery(new BenchmarkNoOpJob(), "job-{$i}", 1, sprintf('App\Jobs\Benchmark%03d', $i % $classes));
        }
        $broker = new MetricsBroker($deliveries);
    } else {
        // Nothing left by an earlier sample: a queue and a namespace of its own.
        $run = bin2hex(random_bytes(4));
        $queue = "bench-job-metrics-{$run}";
        $namespace = "bench-job-metrics-{$run}";
    }
    $connection = [
        'driver' => 'queen',
        'url' => $url ?? 'http://queen.test:6632',
        'queue' => $queue,
        'consumer_group' => 'workers',
        'block_for' => 0,
    ];
    if ($broker !== null) {
        $connection += ['handler' => $broker->handler(), 'retry_attempts' => 1, 'retry_delay' => 0];
    }
    // As a test case's defineEnvironment(): after the providers register, before they boot.
    $factory = new class() extends Application {
        public ?Closure $environment = null;

        protected function defineEnvironment($app)
        {
            ($this->environment)($app);
        }
    };
    $factory->environment = function ($app) use ($connection, $namespace, $mode): void {
        $app['config']->set('queue.default', 'queen');
        $app['config']->set('queue.connections.queen', $connection);
        $app['config']->set('queue.failed.driver', 'null');
        $app['config']->set('queen.sync_failed_jobs', false);
        $app['config']->set('queen.job_metrics.enabled', $mode !== 'off');
        $app['config']->set('queen.job_metrics.namespace', $namespace);
        $app['config']->set('queen.tags.enabled', $mode === 'defaults');
    };
    $app = $factory->configure(['extra' => ['providers' => [QueenServiceProvider::class], 'dont-discover' => ['*']]])->createApplication();
    if ($url !== null) {
        foreach (array_chunk(range(1, $jobs), 500) as $chunk) {
            $app['queue']->connection('queen')->bulk(array_map(fn (): BenchmarkNoOpJob => new BenchmarkNoOpJob(), $chunk), '', $queue);
        }
    }
    $kernel = $app->make(Kernel::class);
    $argv = ['artisan', 'queue:work', 'queen', "--queue={$queue}", '--sleep=0', '--timeout=60', '--tries=3', '--memory=4096',
        '--backoff=0', '--max-jobs=0', '--max-time=0', '--rest=0', '--quiet', '--stop-when-empty'];

    gc_collect_cycles();
    $before = getrusage();
    $started = hrtime(true);
    $code = $kernel->handle(new ArgvInput($argv), $output = new BufferedOutput());
    $wall = (hrtime(true) - $started) / 1e9;
    $after = getrusage();
    if ($code !== 0) {
        throw new RuntimeException("queue:work exited {$code}: " . $output->fetch());
    }

    $result = [
        'mode' => $mode,
        'jobs' => $jobs,
        'cpu_s' => seconds($after, 'ru_utime') - seconds($before, 'ru_utime') + seconds($after, 'ru_stime') - seconds($before, 'ru_stime'),
        'wall_s' => $wall,
        'peak_memory_mb' => round(memory_get_peak_usage(true) / 1048576, 1),
    ];
    if ($broker !== null) {
        $requests = [];
        $writeBytes = [];
        foreach ($broker->requests as $request) {
            $kind = explode('/', $request['path'])[3] ?? $request['path'];
            $requests[$kind] = ($requests[$kind] ?? 0) + 1;
            if ($kind === 'kv') {
                $writeBytes[] = $request['bytes'];
            }
        }

        return [...$result, 'requests' => $requests, 'kv_writes' => count($writeBytes), 'kv_write_bytes' => $writeBytes];
    }

    $reader = new JobMetricsReader(new Queen(['url' => $url]), $namespace);
    $read = $reader->read('1h');
    $class = $read['classes'][0] ?? [];

    return [
        ...$result,
        'read_back' => [
            'processed' => $read['totals']['processed'],
            'average_ms' => $class['average_ms'] ?? null,
            'max_ms' => $class['max_ms'] ?? null,
        ],
        'write_ms' => $mode === 'metrics' ? timeWrites($app['queue']->connection('queen')->getBestEffortQueen(), $namespace) : null,
    ];
}

/** A worker's write, as JobMetricsRecorder::flush() makes it, 50 times. */
function timeWrites(Queen $queen, string $namespace): array
{
    $document = ['classes' => ['App\Jobs\BenchmarkNoOpJob' => ['processed' => 1234, 'failed' => 5, 'runtime_ms' => 98765, 'max_ms' => 4321]]];
    $times = [];
    for ($i = 0; $i < 50; ++$i) {
        $started = hrtime(true);
        $queen->kv()->put($namespace, 'bench/v1/' . sprintf('%010d', $i) . '/' . bin2hex(random_bytes(8)), $document, ['ttlSeconds' => 600]);
        $times[] = (hrtime(true) - $started) / 1e6;
    }
    sort($times);

    return ['median' => round(median($times), 3), 'p95' => round($times[(int) floor(0.95 * (count($times) - 1))], 3)];
}

/** start() and finish() alone, against a broker that answers in-process. */
function recorderAlone(int $jobs, int $classes): array
{
    $broker = new MetricsBroker();
    $queen = $broker->queen();
    $recorder = new JobMetricsRecorder(fn () => $queen, 'queen-metrics');
    $names = array_map(fn (int $i): string => sprintf('App\Jobs\Benchmark%03d', $i), range(0, $classes - 1));
    $job = new class() implements Job {
        public string $name = '';

        public function uuid() { return null; }
        public function getJobId() { return '1'; }
        public function payload() { return []; }
        public function fire() {}
        public function release($delay = 0) {}
        public function isReleased() { return false; }
        public function delete() {}
        public function isDeleted() { return false; }
        public function isDeletedOrReleased() { return false; }
        public function attempts() { return 1; }
        public function hasFailed() { return false; }
        public function markAsFailed() {}
        public function fail($e = null) {}
        public function maxTries() { return null; }
        public function maxExceptions() { return null; }
        public function timeout() { return null; }
        public function retryUntil() { return null; }
        public function getName() { return $this->name; }
        public function resolveName() { return $this->name; }
        public function resolveQueuedJobClass() { return $this->name; }
        public function getConnectionName() { return 'queen'; }
        public function getQueue() { return 'default'; }
        public function getRawBody() { return ''; }
    };

    $started = hrtime(true);
    for ($i = 0; $i < $jobs; ++$i) {
        $job->name = $names[$i % $classes];
        $recorder->start($job);
        $recorder->finish($job, false);
    }
    $elapsed = hrtime(true) - $started;

    return ['jobs' => $jobs, 'ns_per_job' => $elapsed / $jobs, 'writes' => count($broker->requests)];
}

function seconds(array $usage, string $field): float
{
    return $usage["{$field}.tv_sec"] + $usage["{$field}.tv_usec"] / 1e6;
}

function median(array $values): float
{
    sort($values);
    $count = count($values);

    return $count % 2 === 1 ? $values[intdiv($count, 2)] : ($values[$count / 2 - 1] + $values[$count / 2]) / 2;
}
