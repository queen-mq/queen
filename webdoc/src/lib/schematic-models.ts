/**
 * What the schematics compute, apart from how they draw it.
 *
 * Each schematic's times, counts and description are derived here from its
 * props, by the same rules the code it depicts follows. The `.astro`
 * component draws from the result; `markdown-partials.ts` publishes the
 * description in the page's markdown twin. One source for both means the `.md`
 * page and `llms-full.txt` say exactly what the figure shows, including when a
 * page passes props that change the numbers.
 */

// --------------------------------------------------------------------------
// JobLifecycle
// --------------------------------------------------------------------------

export type Engine = "queen" | "horizon";

const JOB_LIFECYCLE_ALT: Record<Engine, string> = {
  queen:
    "The life of one Laravel job on Queen. dispatch() pushes it into a partition of the queue, where it is ready. A pop leases it to one worker for retry_after seconds and handle() runs. If it returns, the worker acknowledges it and it is done. If it throws with tries left, or calls release(), the worker acknowledges the delivery and schedules a broker timer in one transaction, and the timer pushes the job back into the partition after the delay; with no delay the job is pushed back at once. If it fails after its tries, the worker acknowledges it to Queen's dead-letter queue and Laravel writes its failed_jobs row; the two are kept in sync, and queue:retry pushes the job back and removes both. If the worker dies, the lease expires and the job is delivered again.",
  horizon:
    "The life of one Laravel job on Horizon, on the same layout. dispatch() pushes it onto the Redis list queues:default, where it is ready. A pop takes it off the list and adds it to the sorted set queues:default:reserved, scored now plus retry_after, and handle() runs. If it returns, the worker removes it from the reserved set and it is done. If it throws with tries left, or calls release(), it moves to the sorted set queues:default:delayed, scored now plus the delay, and a later pop moves it back onto the list once it is due. If it fails after its tries, Laravel writes its failed_jobs row and Horizon keeps a copy in Redis for its dashboard; queue:retry pushes the job back. If the worker dies, the job stays in the reserved set until retry_after has passed, and a later pop moves it back onto the list.",
};

export function jobLifecycle(props: { engine?: Engine }) {
  const engine = props.engine ?? "queen";
  if (engine !== "queen" && engine !== "horizon") {
    throw new Error(`JobLifecycle: engine must be "queen" or "horizon", not "${engine}".`);
  }
  return { engine, alt: JOB_LIFECYCLE_ALT[engine] };
}

// --------------------------------------------------------------------------
// Lease renewal: the rule in supervisor/src/lease/mod.rs
// --------------------------------------------------------------------------

/** The lease service's retry delay after a failed renewal (RETRY_DELAY_MILLIS). */
export const RENEWAL_RETRY_DELAY = 1;
/** QueenConnector's defaults: lease_renewal_timeout, _kill_grace, _safety_margin. */
export const RENEWAL_DEFAULTS = { timeout: 5, killGrace: 2, safetyMargin: 1 } as const;

/** `lease_renewal_interval` when unset: a third of retry_after, at least 1. */
export function defaultInterval(retryAfter: number): number {
  return Math.max(1, Math.floor(retryAfter / 3));
}

/**
 * What one renewal needs before the lease end: two request budgets (one
 * endpoint), the retry delay, the kill grace and the safety margin.
 */
function renewalReserve(timeout: number, killGrace: number, safetyMargin: number): number {
  return 2 * timeout + RENEWAL_RETRY_DELAY + killGrace + safetyMargin;
}

/** `Timing::next_renewal`: when the renewal after `now` is due. */
function nextRenewal(now: number, end: number, interval: number, reserve: number): number {
  return Math.min(now + interval, Math.max(now, end - reserve));
}

/** The renewals of a lease taken at `popAt`, made while `alive(t)`. */
function renewalsWhile(popAt: number, retryAfter: number, interval: number, reserve: number, alive: (t: number) => boolean) {
  const renewals: number[] = [];
  let end = popAt + retryAfter;
  let next = nextRenewal(popAt, end, interval, reserve);
  while (alive(next)) {
    renewals.push(next);
    end = next + retryAfter;
    next = nextRenewal(next, end, interval, reserve);
  }
  return { renewals, end, next };
}

// --------------------------------------------------------------------------
// LeaseTimeline
// --------------------------------------------------------------------------

export interface LeaseTimelineProps {
  retryAfter?: number;
  interval?: number;
  timeout?: number;
  killGrace?: number;
  safetyMargin?: number;
  jobSeconds?: number;
  outageAt?: number;
}

export function leaseTimeline(props: LeaseTimelineProps) {
  const retryAfter = props.retryAfter ?? 90;
  const interval = props.interval ?? defaultInterval(retryAfter);
  const timeout = props.timeout ?? RENEWAL_DEFAULTS.timeout;
  const killGrace = props.killGrace ?? RENEWAL_DEFAULTS.killGrace;
  const safetyMargin = props.safetyMargin ?? RENEWAL_DEFAULTS.safetyMargin;
  const jobSeconds = props.jobSeconds ?? 140;
  const outageAt = props.outageAt ?? 45;
  const reserve = renewalReserve(timeout, killGrace, safetyMargin);
  if (interval + reserve >= retryAfter) {
    // QueenConnector refuses this configuration; do not draw one it would not run.
    throw new Error(
      "LeaseTimeline: interval + two request budgets + retry + kill grace + safety margin must be shorter than retryAfter.",
    );
  }

  const healthy = renewalsWhile(0, retryAfter, interval, reserve, (t) => t < jobSeconds).renewals;

  // Every renewal from outageAt fails at once (a refused connection) and is
  // retried a second later. The fence comes when an attempt cannot begin, or
  // a retry could not finish, before the lease end less the safety margin.
  const before = renewalsWhile(0, retryAfter, interval, reserve, (t) => t < outageAt);
  const renewed = before.renewals;
  const leaseEnd = before.end;
  const firstFailure = before.next;
  let sigterm = firstFailure;
  while (
    sigterm + timeout + safetyMargin < leaseEnd &&
    sigterm + RENEWAL_RETRY_DELAY + timeout + safetyMargin < leaseEnd
  ) {
    sigterm += RENEWAL_RETRY_DELAY;
  }
  if (sigterm >= jobSeconds) {
    throw new Error("LeaseTimeline: the outage must come early enough to stop the job before it ends.");
  }
  const latestKill = leaseEnd - safetyMargin;
  const sigkill = Math.min(sigterm + killGrace, latestKill);
  const killedByGrace = sigterm + killGrace <= latestKill;
  const secondAttempt = leaseEnd + 1;

  const list = (values: number[]) => values.join(", ");
  const renewedPart = renewed.length
    ? `the renewals at ${list(renewed)} seconds succeeded and moved the lease end to ${leaseEnd} seconds, and`
    : `the lease still ends at ${leaseEnd} seconds, and`;
  const alt = `Two timelines of a job that runs ${jobSeconds} seconds with retry_after ${retryAfter}. With the broker reachable, the supervisor's lease service renews the lease every ${interval} seconds, at ${list(healthy)} seconds, each time to ${retryAfter} seconds ahead, so the lease outlives the job and the worker acknowledges it at ${jobSeconds} seconds; without renewal it would have ended at ${retryAfter} seconds, mid-job. With the broker unreachable from ${outageAt} seconds, ${renewedPart} every renewal from ${firstFailure} seconds fails and is retried a second later. At ${sigterm} seconds no renewal can finish before the lease end minus the ${safetyMargin} second safety margin, so the worker gets SIGTERM; at ${sigkill} seconds it gets SIGKILL. The lease ends at ${leaseEnd} seconds and another worker runs the job again from the start. The two attempts never overlap, and delivery is at-least-once.`;

  return {
    retryAfter,
    interval,
    jobSeconds,
    outageAt,
    healthy,
    renewed,
    leaseEnd,
    firstFailure,
    sigterm,
    sigkill,
    latestKill,
    killedByGrace,
    secondAttempt,
    alt,
  };
}

// --------------------------------------------------------------------------
// RollingUpdate
// --------------------------------------------------------------------------

export interface RollingUpdateProps {
  shutdownGrace?: number;
  terminationGracePeriod?: number;
  retryAfter?: number;
  interval?: number;
  shortJobEnds?: number;
  longJobStarted?: number;
}

export function rollingUpdate(props: RollingUpdateProps) {
  const shutdownGrace = props.shutdownGrace ?? 75;
  const terminationGracePeriod = props.terminationGracePeriod ?? 90;
  const retryAfter = props.retryAfter ?? 90;
  const interval = props.interval ?? defaultInterval(retryAfter);
  const shortJobEnds = props.shortJobEnds ?? 25;
  const longJobStarted = props.longJobStarted ?? 10;
  if (terminationGracePeriod <= shutdownGrace) {
    throw new Error("RollingUpdate: terminationGracePeriod must be longer than shutdownGrace.");
  }
  if (shortJobEnds >= shutdownGrace) {
    throw new Error("RollingUpdate: the short job must end within shutdownGrace.");
  }
  const { timeout, killGrace, safetyMargin } = RENEWAL_DEFAULTS;
  const reserve = renewalReserve(timeout, killGrace, safetyMargin);

  // The master's lease service renews the long job's lease while its worker
  // lives; the SIGKILL at the grace ends the renewals, not the lease.
  const popAt = -longJobStarted;
  const { renewals, end: leaseEnd } = renewalsWhile(popAt, retryAfter, interval, reserve, (t) => t < shutdownGrace);
  const lastRenewal = renewals.at(-1) ?? popAt;
  const rerunAt = leaseEnd + 1;

  const alt = `A rolling update seen from the old pod, starting when Kubernetes sends SIGTERM at 0 seconds. The supervisor sends SIGTERM to every worker and waits up to shutdown_grace, ${shutdownGrace} seconds. A worker whose job ends at ${shortJobEnds} seconds acknowledges it, hands back the prefetched jobs it never started, and exits. A worker whose job is still running at ${shutdownGrace} seconds is killed without an acknowledgement; its lease, last renewed at ${lastRenewal} seconds, ends at ${leaseEnd} seconds, and a worker on the new pod then runs the job again from the start. terminationGracePeriodSeconds, ${terminationGracePeriod} seconds, is longer than shutdown_grace, so Kubernetes never kills the pod before the supervisor's own deadline.`;

  return { shutdownGrace, terminationGracePeriod, retryAfter, shortJobEnds, lastRenewal, leaseEnd, rerunAt, alt };
}

// --------------------------------------------------------------------------
// WorkerTimeline: the request order of QueenQueue
// --------------------------------------------------------------------------

export interface WorkerTimelineProps {
  prefetch?: number;
  jobs?: number;
  roundTrip?: number;
}

export interface Span {
  start: number;
  end: number;
  label: string;
}

export interface Flight extends Span {
  /** 0: an ACK, 1: a pop. They can travel at the same time. */
  lane: 0 | 1;
}

export interface WorkerRow {
  title: string;
  note: string;
  /** Whether the title is a config key, set in code type. */
  code: boolean;
  jobs: Span[];
  waits: Span[];
  flights: Flight[];
  end: number;
}

/** A job's run time; the round trip is a fraction of it. */
export const WORKER_JOB = 10;

export function workerTimeline(props: WorkerTimelineProps) {
  const prefetch = props.prefetch ?? 4;
  const jobs = props.jobs ?? prefetch * 2;
  const roundTrip = props.roundTrip ?? 0.4;
  if (!Number.isInteger(prefetch) || prefetch < 1 || !Number.isInteger(jobs) || jobs < 1) {
    throw new Error("WorkerTimeline: prefetch and jobs must be positive integers.");
  }
  if (!(roundTrip > 0 && roundTrip < 1)) {
    throw new Error("WorkerTimeline: roundTrip must be between 0 and 1 job.");
  }
  const rtt = WORKER_JOB * roundTrip;

  const simulate = (title: string, note: string, ackAsync: boolean, popAhead: boolean): WorkerRow => {
    const row: WorkerRow = { title, note, code: ackAsync, jobs: [], waits: [], flights: [], end: 0 };
    let t = 0;
    let buffered = 0;
    let pendingAck: number | null = null;
    let pendingPop: number | null = null;
    let lastBatchFull = false;
    const waitUntil = (until: number, label: string) => {
      if (until > t) {
        row.waits.push({ start: t, end: until, label });
        t = until;
      }
    };
    for (let n = 1; n <= jobs; n++) {
      // pop(): the local batch first. When it is empty, nextFromBroker()
      // reads the answer to an ACK still on the wire, then pops.
      if (buffered === 0) {
        if (pendingAck !== null) {
          waitUntil(pendingAck + rtt, "ack");
          pendingAck = null;
        }
        waitUntil(t + rtt, "pop");
        buffered = prefetch;
        lastBatchFull = true;
      }
      buffered -= 1;
      // popAhead(): handing out the last job of a full batch sends the next pop.
      if (popAhead && pendingPop === null && buffered === 0 && lastBatchFull) {
        pendingPop = t;
        row.flights.push({ start: t, end: t + rtt, label: "pop", lane: 1 });
      }
      row.jobs.push({ start: t, end: t + WORKER_JOB, label: String(n) });
      t += WORKER_JOB;
      // delete(): the previous ACK's answer, this job's ACK, then the batch
      // popped ahead (settlePendingPop()).
      if (pendingAck !== null) {
        waitUntil(pendingAck + rtt, "ack");
        pendingAck = null;
      }
      if (ackAsync) {
        pendingAck = t;
        row.flights.push({ start: t, end: t + rtt, label: "ack", lane: 0 });
      } else {
        waitUntil(t + rtt, "ack");
      }
      if (pendingPop !== null) {
        waitUntil(pendingPop + rtt, "pop");
        pendingPop = null;
        buffered = prefetch;
        lastBatchFull = true;
      }
    }
    row.end = t;
    return row;
  };

  const rows = [
    simulate("Default", "the worker waits for each pop and each ACK", false, false),
    simulate("ack_async", "an ACK travels while the next job runs", true, false),
    simulate("ack_async + pop_ahead", "the next pop travels while the batch's last job runs", true, true),
  ];
  const trips = (n: number) => `${n} round ${n === 1 ? "trip" : "trips"}`;
  const alt = `Three timelines of one worker running ${jobs} jobs with prefetch ${prefetch}; a round trip to the broker is drawn as ${roundTrip} of a job. By default the worker waits for every pop and every acknowledgement: ${trips(rows[0].waits.length)}. With ack_async each acknowledgement travels while the next job runs, and the worker waits only when its batch runs out, for the last acknowledgement and the next pop: ${trips(rows[1].waits.length)}. With ack_async and pop_ahead the pop for the next batch also travels while the last job of the batch runs: ${trips(rows[2].waits.length)}.`;

  return { prefetch, jobs, roundTrip, rows, alt };
}

// --------------------------------------------------------------------------
// SupervisorTopology: `desired_share` in supervisor/src/main.rs
// --------------------------------------------------------------------------

export function supervisorTopology(props: { target?: number }) {
  const target = props.target ?? 6;
  if (!Number.isInteger(target) || target < 2 || target > 6) {
    throw new Error("SupervisorTopology: target must be an integer from 2 to 6.");
  }
  const pods = 2;
  // The fleet target is shared, the remainder going to the lowest positions.
  const shares = Array.from({ length: pods }, (_, i) => Math.floor(target / pods) + (i < target % pods ? 1 : 0));
  const alt = `Two Kubernetes pods share one broker. In each pod the Rust supervisor master is the main process: it sizes the pools, starts and drains the workers, and renews their leases through its lease service on a Unix socket, lease.sock. A fork server boots Laravel once, and every worker, in pools for the queues default and emails, is forked from it and runs queue:work. Workers pop and acknowledge jobs over HTTP; the master renews leases, reads the queue depth and keeps one coordination key per pod in the broker's key/value store. Both masters compute the same fleet target of ${target} workers from the backlog, see ${pods} replicas, and each runs its share: ${shares.join(" and ")}.`;
  return { target, pods, shares, alt };
}

// --------------------------------------------------------------------------
// PreforkMemory: measured, not illustrative
// --------------------------------------------------------------------------

export const PREFORK_MEMORY_SOURCE = "benchmark-queen/2026-10-01-linux-vm-horizon-raft/raw/process-memory.csv";

/** Lane drain-64, role worker: MiB each, and the proportional set size of all of them. */
const PREFORK_MEASURED = {
  horizon: { workers: 64, resident: 48.0, private: 28.2, pssTotal: 1825.3 },
  queen: { workers: 64, resident: 32.7, private: 1.4, pssTotal: 121.2 },
} as const;

export function preforkMemory() {
  const withShared = <T extends { resident: number; private: number }>(m: T) => ({
    ...m,
    // Shared is resident minus private, to the CSV's one decimal.
    shared: Math.round((m.resident - m.private) * 10) / 10,
  });
  const horizon = withShared(PREFORK_MEASURED.horizon);
  const queen = withShared(PREFORK_MEASURED.queen);
  const total = (value: number) => `${Math.round(value).toLocaleString("en-US")} MiB`;
  const alt = `Memory of workers drawn to one scale, five of 64 shown. Each Horizon worker boots its own Laravel: ${horizon.resident.toFixed(1)} MiB resident, of which ${horizon.private.toFixed(1)} MiB is private to it and ${horizon.shared.toFixed(1)} MiB shared with other processes; the 64 workers' proportional set size is ${total(horizon.pssTotal)}. Each Queen worker is forked from one Laravel booted in the fork server and shares its pages until it writes them: ${queen.resident.toFixed(1)} MiB resident, of which ${queen.private.toFixed(1)} MiB is private and ${queen.shared.toFixed(1)} MiB shared; the 64 workers' proportional set size is ${total(queen.pssTotal)}.`;
  return { horizon, queen, source: PREFORK_MEMORY_SOURCE, total, alt };
}

// --------------------------------------------------------------------------
// The markdown twin
// --------------------------------------------------------------------------

type Attrs = Record<string, string | boolean>;

/** A numeric prop as the downleveler hands it over: `retryAfter={60}` is "60". */
function num(attrs: Attrs, key: string, component: string): number | undefined {
  const value = attrs[key];
  if (value === undefined) return undefined;
  const parsed = typeof value === "string" ? Number(value) : Number.NaN;
  if (!Number.isFinite(parsed)) {
    // The HTML would compute from the real value; refuse to publish a
    // markdown description computed from a different one.
    throw new Error(`<${component} ${key}=…/>: the markdown twin can only read a literal number, got ${String(value)}.`);
  }
  return parsed;
}

/** The description and provenance a schematic carries into markdown. */
export function describeSchematic(name: string, attrs: Attrs): { alt: string; source?: string } {
  const n = (key: string) => num(attrs, key, name);
  switch (name) {
    case "JobLifecycle":
      return jobLifecycle({ engine: (typeof attrs.engine === "string" ? attrs.engine : undefined) as Engine | undefined });
    case "LeaseTimeline":
      return leaseTimeline({
        retryAfter: n("retryAfter"),
        interval: n("interval"),
        timeout: n("timeout"),
        killGrace: n("killGrace"),
        safetyMargin: n("safetyMargin"),
        jobSeconds: n("jobSeconds"),
        outageAt: n("outageAt"),
      });
    case "RollingUpdate":
      return rollingUpdate({
        shutdownGrace: n("shutdownGrace"),
        terminationGracePeriod: n("terminationGracePeriod"),
        retryAfter: n("retryAfter"),
        interval: n("interval"),
        shortJobEnds: n("shortJobEnds"),
        longJobStarted: n("longJobStarted"),
      });
    case "WorkerTimeline":
      return workerTimeline({ prefetch: n("prefetch"), jobs: n("jobs"), roundTrip: n("roundTrip") });
    case "SupervisorTopology":
      return supervisorTopology({ target: n("target") });
    case "PreforkMemory": {
      const { alt, source } = preforkMemory();
      return { alt, source };
    }
    default:
      throw new Error(`describeSchematic: no model for <${name} />.`);
  }
}

/** Every schematic with a markdown description, for the exporter and its check. */
export const SCHEMATICS = [
  "JobLifecycle",
  "LeaseTimeline",
  "RollingUpdate",
  "WorkerTimeline",
  "SupervisorTopology",
  "PreforkMemory",
] as const;
