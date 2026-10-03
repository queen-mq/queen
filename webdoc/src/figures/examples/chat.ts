import { flow } from "@/lib/figure-spec";

// examples/chat.mdx, "How it works".
export default flow({
  alt: "The chat backend. Phones push each message into the chat queue, into the partition named after its conversation (conv-en-1, conv-en-2, conv-jp-1), with the phone's own message id as transactionId so a resend is stored once. Three consumer groups read the same partitions, each with its own cursors: delivery marks messages delivered, enrichment translates them, and sentiment, created after every message was sent, reads the whole history from the oldest retained message.",
  caption: "One copy of every message and a cursor per group. Slow translation of one conversation delays that conversation in that group, and nothing else.",
  cols: 3,
  rows: 3,
  colWidth: 228,
  rowHeight: 80,
  gap: 70,
  nodes: [
    { id: "phones", at: [0, 1], label: "phones", sub: "push, id = message id", tone: "ghost", shape: "pill" },
    { id: "queue", at: [1, 1], label: "queue chat", sub: "a partition per\nconversation", shape: "stack" },
    { id: "g1", at: [2, 0], label: "delivery", sub: "marks delivered", tone: "ghost", shape: "pill" },
    { id: "g2", at: [2, 1], label: "enrichment", sub: "translates", tone: "ghost", shape: "pill" },
    { id: "g3", at: [2, 2], label: "sentiment", sub: "added later: all history", tone: "ghost", shape: "pill" },
  ],
  edges: [
    { from: "phones", to: "queue", label: "push", tone: "strong" },
    { from: "queue", to: "g1", fromSide: "r", toSide: "l" },
    { from: "queue", to: "g2", fromSide: "r", toSide: "l" },
    { from: "queue", to: "g3", fromSide: "r", toSide: "l", dashed: true },
  ],
});
