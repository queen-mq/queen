import { sequence } from "@/lib/figure-spec";

// benchmarks/methodology.mdx, "The load generators".
export default sequence({
  alt: "How one unit of load is timed. A producer schedules a unit of 100 messages for one partition at instant t0. It is sent if the process has fewer than 5,000 units in flight, otherwise it is shed and counted, never queued. The broker acknowledges it, and produce latency is the acknowledgement time minus t0. A consumer later receives the messages, which carry t0, and e2e latency is the receive time minus t0, taken before the ack.",
  caption: "Both latencies start at the scheduled instant, not at the send, so a broker that slows down shows up as latency and shedding instead of as a smaller load.",
  source: "benchmark-queen/2026-09-30-kafka-pulsar (goload, kload, pload)",
  gap: 210,
  actors: [
    { id: "p", label: "producer", sub: "load machine", tone: "ghost" },
    { id: "b", label: "broker", sub: "three nodes", tone: "strong" },
    { id: "c", label: "consumer", sub: "load machine", tone: "ghost" },
  ],
  steps: [
    { note: "unit scheduled at t0:\n100 messages, one partition", over: "p" },
    { from: "p", to: "b", label: "push, if under 5,000 in flight", sub: "otherwise shed and counted" },
    { from: "b", to: "p", label: "ack", reply: true },
    { note: "produce latency = ack − t0", over: "p", tone: "strong" },
    { from: "c", to: "b", label: "pop or fetch" },
    { from: "b", to: "c", label: "messages carrying t0", reply: true },
    { note: "e2e = receive − t0,\nbefore the ack", over: "c", tone: "strong" },
  ],
});
