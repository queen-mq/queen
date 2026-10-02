<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Queue\HandBackJournal;

class HandBackJournalTest extends TestCase
{
    private string $directory;

    private string $prefix;

    protected function setUp(): void
    {
        parent::setUp();
        $this->directory = sys_get_temp_dir() . '/qhbj-' . bin2hex(random_bytes(4));
        mkdir($this->directory, 0700);
        $this->prefix = "{$this->directory}/hand-back-42";
    }

    protected function tearDown(): void
    {
        foreach (glob($this->directory . '/*') ?: [] as $file) {
            @unlink($file);
        }
        @rmdir($this->directory);
        parent::tearDown();
    }

    public function testAPlanOwesNothingUntilTheWorkerSaysWhat(): void
    {
        $journal = new HandBackJournal($this->prefix);

        $this->assertTrue($journal->plan('lease-1', [$this->entry(1), $this->entry(2)]));

        $plan = json_decode((string) file_get_contents("{$this->prefix}.plan"), true);
        $this->assertSame(['lease_id' => 'lease-1', 'entries' => [$this->entry(1), $this->entry(2)]], $plan);
        $this->assertSame('', file_get_contents("{$this->prefix}.state"));
        $this->assertFileDoesNotExist("{$this->prefix}.plan.tmp");
    }

    public function testEachStateOfABatchIsVisibleAtOnceAndHasTheSameLength(): void
    {
        $journal = new HandBackJournal($this->prefix);
        $journal->plan('lease-1', [$this->entry(1), $this->entry(2), $this->entry(3)]);

        $lengths = [];
        foreach (['uuu', 'ruu', '-uu', 'cru'] as $codes) {
            $journal->owe($codes);
            // Read through another descriptor, as the supervisor would.
            $record = (string) file_get_contents("{$this->prefix}.state");
            $this->assertSame(['lease_id' => 'lease-1', 'entries' => $codes], json_decode($record, true));
            $lengths[] = strlen($record);
        }
        $this->assertCount(1, array_unique($lengths));

        $journal->owe('uu');
        $this->assertSame('cru', json_decode((string) file_get_contents("{$this->prefix}.state"), true)['entries'],
            'a state of another length is not this batch\'s');
    }

    public function testANewBatchClearsTheStateBeforeItsPlanReplacesTheLastOne(): void
    {
        $journal = new HandBackJournal($this->prefix);
        $journal->plan('lease-1', [$this->entry(1), $this->entry(2), $this->entry(3)]);
        $journal->owe('ruu');

        $journal->plan('lease-2', [$this->entry(4)]);

        $this->assertSame('', file_get_contents("{$this->prefix}.state"));
        $journal->owe('u');
        $this->assertSame(
            ['lease_id' => 'lease-2', 'entries' => 'u'],
            json_decode((string) file_get_contents("{$this->prefix}.state"), true),
        );
    }

    public function testWithdrawingOwesNothingAndDiscardingRemovesTheFiles(): void
    {
        $journal = new HandBackJournal($this->prefix);
        $journal->plan('lease-1', [$this->entry(1), $this->entry(2)]);
        $journal->owe('ru');

        $journal->withdraw();
        $this->assertSame('--', json_decode((string) file_get_contents("{$this->prefix}.state"), true)['entries']);
        $journal->owe('uu');
        $this->assertSame('--', json_decode((string) file_get_contents("{$this->prefix}.state"), true)['entries'],
            'a withdrawn batch owes nothing again');

        $journal->discard();
        $this->assertSame([], glob($this->directory . '/*'));
    }

    public function testABatchWhoseStateCannotFitOneWriteIsNotJournaled(): void
    {
        $journal = new HandBackJournal($this->prefix);
        $entries = array_fill(0, 4100, $this->entry(1));

        $this->assertFalse($journal->plan('lease-1', $entries));
        $journal->owe(str_repeat('u', 4100));
        $this->assertSame('', file_get_contents("{$this->prefix}.state"));
    }

    public function testAJournalThatCannotBeWrittenStopsForGood(): void
    {
        $journal = new HandBackJournal("{$this->directory}/missing/hand-back-42");
        $log = "{$this->directory}/error.log";
        $previous = ini_set('error_log', $log);
        try {
            $this->assertFalse($journal->plan('lease-1', [$this->entry(1)]));
            mkdir("{$this->directory}/missing", 0700);
            $this->assertFalse($journal->plan('lease-1', [$this->entry(1)]));
        } finally {
            ini_set('error_log', (string) $previous);
        }
        $this->assertSame([], glob("{$this->directory}/missing/*"));
        rmdir("{$this->directory}/missing");
        $this->assertStringContainsString('a crash charges each one an attempt', (string) file_get_contents($log));
    }

    public function testWhatIsOwedFollowsTheStateInPlanOrder(): void
    {
        $journal = new HandBackJournal($this->prefix);
        $journal->plan('lease-1', [$this->entry(1), $this->entry(2), $this->entry(3), $this->entry(4)]);
        $journal->owe('cru-');

        $owed = HandBackJournal::owed($this->prefix, 'lease-1');

        $this->assertSame([
            'operations' => [
                $this->entry(1)['ack'],
                $this->entry(2)['ack'], $this->entry(2)['ran'],
                $this->entry(3)['ack'], $this->entry(3)['unstarted'],
            ],
            'unstarted' => 1,
            'running' => 1,
        ], $owed);
    }

    public function testNothingIsOwedWithoutAStateForTheLeaseOrWithNothingInIt(): void
    {
        $this->assertNull(HandBackJournal::owed($this->prefix, 'lease-1'), 'no journal');

        $journal = new HandBackJournal($this->prefix);
        $journal->plan('lease-1', [$this->entry(1), $this->entry(2)]);
        $this->assertNull(HandBackJournal::owed($this->prefix, 'lease-1'), 'a plan, no state yet');
        $journal->owe('--');
        $this->assertNull(HandBackJournal::owed($this->prefix, 'lease-1'), 'nothing owed');
        $journal->owe('uu');
        $this->assertNull(HandBackJournal::owed($this->prefix, 'lease-2'), 'another lease');

        HandBackJournal::remove($this->prefix);
        $this->assertSame([], glob($this->directory . '/*'));
    }

    public function testAJournalThatDoesNotHoldTogetherIsRefused(): void
    {
        $journal = new HandBackJournal($this->prefix);
        $journal->plan('lease-1', [$this->entry(1), $this->entry(2)]);

        $refused = function (string $state, string $why): void {
            file_put_contents("{$this->prefix}.state", $state);
            try {
                HandBackJournal::owed($this->prefix, 'lease-1');
                $this->fail("Accepted {$why}.");
            } catch (\RuntimeException) {
                $this->addToAssertionCount(1);
            }
        };
        $refused('{"lease_id":"lease-1","entries":"uuu"}', 'a state for another batch');
        $refused('{"lease_id":"lease-1","entries":"ux"}', 'an unknown code');
        $refused('{"lease_id":"lease-1","entries":"uu"}"u"}', 'a torn record');

        $other = $this->entry(1);
        $other['ack']['leaseId'] = 'lease-2';
        $journal->plan('lease-1', [$other]);
        $refused('{"lease_id":"lease-1","entries":"u"}', 'an ACK another lease fences');
    }

    private function entry(int $number): array
    {
        return [
            'ack' => ['type' => 'ack', 'transactionId' => "transaction-{$number}", 'status' => 'completed', 'leaseId' => 'lease-1'],
            'unstarted' => ['type' => 'push', 'items' => [['payload' => ['uuid' => "job-{$number}"]]]],
            'ran' => ['type' => 'push', 'items' => [['payload' => ['uuid' => "job-{$number}", 'ran' => true]]]],
        ];
    }
}
