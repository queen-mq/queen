<?php
// Boot Laravel once, then fork N children that each run queue:work.
$n = (int) ($argv[1] ?? 4);
require '/app/vendor/autoload.php';
$app = require '/app/bootstrap/app.php';
$kernel = $app->make(Illuminate\Contracts\Console\Kernel::class);
$kernel->bootstrap();
// Warm the worker's own classes without opening broker connections.
class_exists(Illuminate\Queue\Worker::class);
class_exists(Illuminate\Queue\Console\WorkCommand::class);
$app->make("queue.worker");
$children = [];
for ($i = 0; $i < $n; $i++) {
    $pid = pcntl_fork();
    if ($pid === 0) {
        exit($kernel->call('queue:work', ['connection' => 'queen', '--queue' => getenv('SPIKE_QUEUE'), '--sleep' => 1, '--quiet' => true]));
    }
    $children[] = $pid;
}
file_put_contents('/tmp/prefork-children', implode(' ', $children));
foreach ($children as $pid) {
    pcntl_waitpid($pid, $status);
}
