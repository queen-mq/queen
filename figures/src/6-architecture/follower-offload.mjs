// Chapter 6.1: a follower does the work of its own clients.
export default {
  id: "follower-offload",
  height: 340,
  alt: "A client sends a push to a follower. The follower parses it and prepares the command, and sends the leader only the prepared command. The leader orders it into an entry, and once a majority has it on disk the entry is committed. The follower applies the entry to its own copy and answers its client from there. The leader never parses the client's JSON.",
  draw(c) {
    c.sequence(
      [{ id: "c", label: "client" }, { id: "f", label: "follower", sub: "parses, prepares" }, { id: "l", label: "leader", tone: "strong", sub: "orders, writes" }],
      [
        { from: "c", to: "f", label: "push (JSON)" },
        { from: "f", to: "f", label: "parse, prepare the command" },
        { from: "f", to: "l", label: "prepared command" },
        { note: "one entry, a majority on disk: committed", over: "l", tone: "accent" },
        { from: "l", to: "f", label: "the entry", reply: true },
        { from: "f", to: "f", label: "apply to its own copy" },
        { from: "f", to: "c", label: "answer, from its own copy", reply: true, tone: "ok" },
      ],
      { y: 10, rowH: 38 },
    );
  },
};
