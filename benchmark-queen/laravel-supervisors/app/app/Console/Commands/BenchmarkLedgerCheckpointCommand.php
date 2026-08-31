<?php

namespace App\Console\Commands;

use App\Support\BenchmarkEffectLedger;
use Illuminate\Console\Command;
use JsonException;
use RuntimeException;

final class BenchmarkLedgerCheckpointCommand extends Command
{
    protected $signature = 'bench:ledger-checkpoint {run-id}';

    protected $description = 'Checkpoint a quiescent durable benchmark ledger before artifact copy';

    public function handle(BenchmarkEffectLedger $ledger): int
    {
        $runId = (string) $this->argument('run-id');
        try {
            $this->line(json_encode(
                ['run_id' => $runId] + $ledger->checkpointRun($runId),
                JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES,
            ));
        } catch (JsonException $exception) {
            throw new RuntimeException('Unable to encode ledger checkpoint result.', previous: $exception);
        }

        return self::SUCCESS;
    }
}
