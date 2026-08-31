<?php

namespace Queen\Http;

class LoadBalancer
{
    private array $urls;
    private string $strategy;
    private int $currentIndex = 0;
    private array $sessionMap = [];
    private string $sessionId;
    private array $healthStatus = [];
    private ?array $virtualNodes = null;
    private int $affinityHashRing;
    private int $healthRetryAfterMillis;

    public function __construct(array $urls, string $strategy = 'round-robin', array $options = [])
    {
        $this->urls = array_map(fn(string $url) => rtrim($url, '/'), $urls);
        $this->strategy = $strategy;
        $this->sessionId = 'session_' . time() . '_' . bin2hex(random_bytes(5));
        $this->affinityHashRing = $options['affinityHashRing'] ?? 150;
        $this->healthRetryAfterMillis = $options['healthRetryAfterMillis'] ?? 30000;

        foreach ($this->urls as $url) {
            $this->healthStatus[$url] = [
                'healthy' => true,
                'failures' => 0,
                'lastFailure' => null,
            ];
        }

        if ($strategy === 'affinity') {
            $this->buildVirtualNodeRing();
        }
    }

    private function buildVirtualNodeRing(?array $urls = null): void
    {
        $urlsToUse = $urls ?? $this->urls;
        $this->virtualNodes = [];

        foreach ($urlsToUse as $url) {
            for ($i = 0; $i < $this->affinityHashRing; $i++) {
                $vnodeKey = "{$url}#vnode{$i}";
                $this->virtualNodes[] = [
                    'hash' => $this->hashString($vnodeKey),
                    'realServer' => $url,
                ];
            }
        }

        usort($this->virtualNodes, fn($a, $b) => $a['hash'] <=> $b['hash']);
    }

    public function getNextUrl(?string $sessionKey = null): string
    {
        $key = $sessionKey ?? $this->sessionId;
        $now = (int)(microtime(true) * 1000);
        $needRingRebuild = false;

        $healthyUrls = array_values(array_filter($this->urls, function (string $url) use ($now, &$needRingRebuild) {
            $status = $this->healthStatus[$url];
            if ($status['healthy']) {
                return true;
            }
            if ($status['lastFailure'] !== null && ($now - $status['lastFailure']) >= $this->healthRetryAfterMillis) {
                $needRingRebuild = true;
                return true;
            }
            return false;
        }));

        if ($needRingRebuild && $this->virtualNodes !== null) {
            $this->buildVirtualNodeRing($healthyUrls);
        }

        if (empty($healthyUrls)) {
            return $this->urls[$this->currentIndex % count($this->urls)];
        }

        if ($this->strategy === 'affinity') {
            return $this->getAffinityUrl($key, $healthyUrls);
        }

        if ($this->strategy === 'session') {
            if (!isset($this->sessionMap[$key])) {
                $this->sessionMap[$key] = $this->currentIndex;
                $this->currentIndex = ($this->currentIndex + 1) % count($healthyUrls);
            }

            $assignedUrl = $this->urls[$this->sessionMap[$key]] ?? $healthyUrls[0];
            if (!in_array($assignedUrl, $healthyUrls, true)) {
                $newIndex = $this->currentIndex % count($healthyUrls);
                $this->sessionMap[$key] = $newIndex;
                $this->currentIndex = ($this->currentIndex + 1) % count($healthyUrls);
                return $healthyUrls[$newIndex];
            }

            return $assignedUrl;
        }

        // Round robin
        $url = $healthyUrls[$this->currentIndex % count($healthyUrls)];
        $this->currentIndex = ($this->currentIndex + 1) % count($healthyUrls);
        return $url;
    }

    private function getAffinityUrl(string $key, array $healthyUrls): string
    {
        if (empty($this->virtualNodes)) {
            $url = $healthyUrls[$this->currentIndex % count($healthyUrls)];
            $this->currentIndex = ($this->currentIndex + 1) % count($healthyUrls);
            return $url;
        }

        $keyHash = $this->hashString($key);

        // Binary search for first vnode >= keyHash
        $left = 0;
        $right = count($this->virtualNodes) - 1;
        $result = 0;

        while ($left <= $right) {
            $mid = intdiv($left + $right, 2);
            if ($this->virtualNodes[$mid]['hash'] >= $keyHash) {
                $result = $mid;
                $right = $mid - 1;
            } else {
                $left = $mid + 1;
            }
        }

        // Walk forward to find first healthy server
        $count = count($this->virtualNodes);
        for ($i = 0; $i < $count; $i++) {
            $idx = ($result + $i) % $count;
            $vnode = $this->virtualNodes[$idx];
            if (in_array($vnode['realServer'], $healthyUrls, true)) {
                return $vnode['realServer'];
            }
        }

        return $healthyUrls[0];
    }

    /**
     * FNV-1a hash function matching the JS implementation.
     * All intermediate operations masked to 32-bit to prevent
     * 64-bit PHP integer overflow producing wrong results.
     */
    private function hashString(string $str): int
    {
        $hash = 2166136261; // FNV offset basis

        for ($i = 0, $len = strlen($str); $i < $len; $i++) {
            $hash = ($hash ^ ord($str[$i])) & 0xFFFFFFFF;
            // Replicate JS: hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24)
            $hash = ($hash
                + (($hash << 1) & 0xFFFFFFFF)
                + (($hash << 4) & 0xFFFFFFFF)
                + (($hash << 7) & 0xFFFFFFFF)
                + (($hash << 8) & 0xFFFFFFFF)
                + (($hash << 24) & 0xFFFFFFFF)
            ) & 0xFFFFFFFF;
        }

        return $hash;
    }

    public function markUnhealthy(string $url): void
    {
        if (!isset($this->healthStatus[$url])) {
            return;
        }

        $this->healthStatus[$url]['healthy'] = false;
        $this->healthStatus[$url]['failures']++;
        $this->healthStatus[$url]['lastFailure'] = (int)(microtime(true) * 1000);

        if ($this->virtualNodes !== null) {
            $healthyUrls = array_values(array_filter($this->urls, fn($u) => $this->healthStatus[$u]['healthy']));
            $this->buildVirtualNodeRing($healthyUrls);
        }
    }

    public function markHealthy(string $url): void
    {
        if (!isset($this->healthStatus[$url])) {
            return;
        }

        $wasUnhealthy = !$this->healthStatus[$url]['healthy'];
        $this->healthStatus[$url]['healthy'] = true;
        $this->healthStatus[$url]['failures'] = 0;
        $this->healthStatus[$url]['lastFailure'] = null;

        if ($this->virtualNodes !== null && $wasUnhealthy) {
            $this->buildVirtualNodeRing();
        }
    }

    public function getHealthStatus(): array
    {
        return $this->healthStatus;
    }

    public function getAllUrls(): array
    {
        return $this->urls;
    }

    public function getStrategy(): string
    {
        return $this->strategy;
    }
}
