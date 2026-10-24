// Chapter 5.1: a write is answered when two of three nodes have it on disk.
export default {
  id: "cluster-write",
  height: 400,
  alt: "A client pushes to the leader of a three-node cluster. The leader writes the entry and fsyncs it, and sends it to both followers. As soon as one follower has written and fsynced it, two of three nodes hold it on disk: it is committed and the client gets its answer. The second follower's confirmation arrives later and changes nothing.",
  draw(c) {
    c.sequence(
      [{ id: "c", label: "client" }, { id: "l", label: "leader", tone: "strong" }, { id: "f1", label: "follower 1" }, { id: "f2", label: "follower 2" }],
      [
        { from: "c", to: "l", label: "push" },
        { from: "l", to: "l", label: "write + fsync" },
        { from: "l", to: "f1", label: "append" },
        { from: "l", to: "f2", label: "append" },
        { from: "f1", to: "l", label: "written + fsynced", reply: true },
        { note: "2 of 3 on disk: committed", over: "l", tone: "accent" },
        { from: "l", to: "c", label: "answer", reply: true, tone: "ok" },
        { from: "f2", to: "l", label: "later: changes nothing", reply: true, tone: "faint" },
      ],
      { y: 10, rowH: 38, x0: 90, x1: 910 },
    );
  },
};
