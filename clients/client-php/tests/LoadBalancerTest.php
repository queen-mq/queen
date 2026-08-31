<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Http\LoadBalancer;

class LoadBalancerTest extends TestCase
{
    public function testRoundRobinCyclesThroughUrls(): void
    {
        $lb = new LoadBalancer(['http://a', 'http://b', 'http://c'], 'round-robin');

        $results = [];
        for ($i = 0; $i < 6; $i++) {
            $results[] = $lb->getNextUrl();
        }

        $this->assertSame('http://a', $results[0]);
        $this->assertSame('http://b', $results[1]);
        $this->assertSame('http://c', $results[2]);
        $this->assertSame('http://a', $results[3]);
        $this->assertSame('http://b', $results[4]);
        $this->assertSame('http://c', $results[5]);
    }

    public function testAffinityReturnsSameServerForSameKey(): void
    {
        $lb = new LoadBalancer(
            ['http://a', 'http://b', 'http://c'],
            'affinity',
            ['affinityHashRing' => 150]
        );

        $url1 = $lb->getNextUrl('user-123');
        $url2 = $lb->getNextUrl('user-123');
        $url3 = $lb->getNextUrl('user-123');

        $this->assertSame($url1, $url2);
        $this->assertSame($url2, $url3);
    }

    public function testAffinityDistributesDifferentKeys(): void
    {
        $lb = new LoadBalancer(
            ['http://a', 'http://b', 'http://c'],
            'affinity',
            ['affinityHashRing' => 150]
        );

        $servers = [];
        for ($i = 0; $i < 100; $i++) {
            $url = $lb->getNextUrl("key-{$i}");
            $servers[$url] = ($servers[$url] ?? 0) + 1;
        }

        // All three servers should get some traffic
        $this->assertCount(3, $servers, 'Affinity should distribute across all servers');
    }

    public function testMarkUnhealthySkipsServer(): void
    {
        $lb = new LoadBalancer(['http://a', 'http://b'], 'round-robin');

        $lb->markUnhealthy('http://a');

        // All requests should go to http://b
        for ($i = 0; $i < 5; $i++) {
            $this->assertSame('http://b', $lb->getNextUrl());
        }
    }

    public function testMarkHealthyRestoresServer(): void
    {
        $lb = new LoadBalancer(['http://a', 'http://b'], 'round-robin');

        $lb->markUnhealthy('http://a');
        $this->assertSame('http://b', $lb->getNextUrl());

        $lb->markHealthy('http://a');

        // Both servers should be used again
        $urls = [];
        for ($i = 0; $i < 4; $i++) {
            $urls[] = $lb->getNextUrl();
        }
        $this->assertContains('http://a', $urls);
        $this->assertContains('http://b', $urls);
    }

    public function testAllUnhealthyFallsBackToAnyServer(): void
    {
        $lb = new LoadBalancer(['http://a', 'http://b'], 'round-robin');

        $lb->markUnhealthy('http://a');
        $lb->markUnhealthy('http://b');

        // Should still return something rather than crash
        $url = $lb->getNextUrl();
        $this->assertContains($url, ['http://a', 'http://b']);
    }

    public function testFnvHashConsistency(): void
    {
        // Same inputs should always produce same routing
        $lb = new LoadBalancer(['http://a', 'http://b', 'http://c'], 'affinity');

        $mapping = [];
        for ($i = 0; $i < 50; $i++) {
            $mapping["key-{$i}"] = $lb->getNextUrl("key-{$i}");
        }

        // Second pass should match exactly
        foreach ($mapping as $key => $expectedUrl) {
            $this->assertSame($expectedUrl, $lb->getNextUrl($key), "Affinity broken for {$key}");
        }
    }

    public function testAffinityReroutesWhenServerUnhealthy(): void
    {
        $lb = new LoadBalancer(['http://a', 'http://b', 'http://c'], 'affinity');

        $url = $lb->getNextUrl('test-key');
        $lb->markUnhealthy($url);

        $newUrl = $lb->getNextUrl('test-key');
        $this->assertNotSame($url, $newUrl, 'Should route to different server after marking unhealthy');
    }

    public function testSessionStickyRouting(): void
    {
        $lb = new LoadBalancer(['http://a', 'http://b', 'http://c'], 'session');

        $url = $lb->getNextUrl('session-1');

        // Same session key should always go to same server
        for ($i = 0; $i < 5; $i++) {
            $this->assertSame($url, $lb->getNextUrl('session-1'));
        }
    }

    public function testTrailingSlashesStripped(): void
    {
        $lb = new LoadBalancer(['http://a/', 'http://b/'], 'round-robin');

        $urls = $lb->getAllUrls();
        $this->assertSame('http://a', $urls[0]);
        $this->assertSame('http://b', $urls[1]);
    }

    public function testHealthStatusTracking(): void
    {
        $lb = new LoadBalancer(['http://a', 'http://b'], 'round-robin');

        $status = $lb->getHealthStatus();
        $this->assertTrue($status['http://a']['healthy']);
        $this->assertTrue($status['http://b']['healthy']);
        $this->assertSame(0, $status['http://a']['failures']);

        $lb->markUnhealthy('http://a');
        $status = $lb->getHealthStatus();
        $this->assertFalse($status['http://a']['healthy']);
        $this->assertSame(1, $status['http://a']['failures']);
        $this->assertNotNull($status['http://a']['lastFailure']);
    }
}
