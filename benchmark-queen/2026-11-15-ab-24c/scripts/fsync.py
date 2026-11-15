import os, time
fd = os.open("/root/fsync-test.bin", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
os.posix_fallocate(fd, 0, 256 << 20)
buf = os.urandom(32768)
ts = []
t_end = time.perf_counter() + 12
while time.perf_counter() < t_end:
    t = time.perf_counter(); os.write(fd, buf); os.fdatasync(fd); ts.append((time.perf_counter() - t) * 1000)
    if os.lseek(fd, 0, os.SEEK_CUR) > (255 << 20): os.lseek(fd, 0, os.SEEK_SET)
os.close(fd); os.unlink("/root/fsync-test.bin")
ts.sort(); n = len(ts)
q = lambda p: ts[min(n - 1, int(p * n))]
print(f"32 KB write + fdatasync, 12 s: {n} syncs ({n/12:.0f}/s) | ms p50 {q(.5):.2f} p90 {q(.9):.2f} p99 {q(.99):.2f} p99.9 {q(.999):.2f} max {ts[-1]:.1f}")
