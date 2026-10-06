import { sequence } from "@/lib/figure-spec";

// guides/laravel/concepts.mdx, "How a forked worker talks to its master".
export default sequence({
  alt: "The life of a forked worker. The master starts the fork server with a pipe on its stdin and another on fd 3. The fork server boots Laravel once, opens no connection, and answers ready. For each worker the master sends a fork request with the queue:work arguments and environment; the fork server forks a child, which shares the opcache and the booted heap, and answers forked with its pid. The worker pops, runs and acknowledges jobs over HTTP with Queen, like any worker. It sends the lease it holds to the master over a Unix socket, and the master renews that lease with Queen. It leaves its job runtimes and its exit reason in files of the master's state directory. When the worker exits, the fork server reaps it and reports its pid and exit status, and the master asks for a new worker. After php artisan queue:restart, the master starts a new fork server and closes the old one when its last worker exits. At shutdown the master sends SIGTERM, then SIGKILL, to each worker's process group. If the master dies, the fork server SIGKILLs every worker it forked.",
  caption: "The worker is the fork server's child, so its birth and its exit travel on the fork server's pipes; everything else goes straight between the worker and the master, as with a worker started on its own.",
  source: "supervisor/src/prefork.rs, clients/client-php/src/Laravel/Supervisor/Prefork/ForkServer.php",
  gap: 180,
  actors: [
    { id: "m", label: "master", sub: "queen:supervisor" },
    { id: "s", label: "fork server", sub: "queen:fork-server", tone: "strong" },
    { id: "w", label: "worker", sub: "forked queue:work" },
    { id: "q", label: "Queen", tone: "ghost" },
  ],
  steps: [
    { from: "m", to: "s", label: "start", sub: "pipes: stdin and fd 3" },
    { from: "s", to: "s", label: "boot Laravel once" },
    { from: "s", to: "m", label: "ready", reply: true },
    { from: "m", to: "s", label: "fork: argv + env" },
    { from: "s", to: "w", label: "fork()", sub: "opcache + heap shared", tone: "strong" },
    { from: "s", to: "m", label: "forked: pid", reply: true },
    { from: "w", to: "q", label: "pop, run, ACK", sub: "HTTP" },
    { from: "w", to: "m", label: "track the lease", sub: "Unix socket" },
    { from: "m", to: "q", label: "renew the lease" },
    { from: "w", to: "m", label: "runtimes, exit reason", sub: "files", tone: "faint" },
    { divider: "the worker exits" },
    { from: "s", to: "m", label: "exited: pid, status" },
    { from: "m", to: "s", label: "fork a new one" },
    { divider: "php artisan queue:restart" },
    { note: "a new fork server boots the code on disk;\nthe old one closes with its last worker", over: ["m", "s"] },
    { divider: "shutdown" },
    { from: "m", to: "w", label: "SIGTERM, then SIGKILL", sub: "the worker's process group" },
    { divider: "the master dies" },
    { note: "SIGKILL every worker it forked", over: "s", tone: "danger" },
  ],
});
