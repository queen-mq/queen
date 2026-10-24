// Chapter 6.3: the timings Raft runs on.
export default {
  id: "raft-timings",
  height: 300,
  alt: "Raft's timings in a cluster. The leader sends a heartbeat every 100 milliseconds. Followers that hear nothing for 1 to 2 seconds elect a new leader. A leader that hears from no majority for 1.5 seconds stops logging new writes and keeps them waiting in order; after 3 seconds it hands leadership to another node. A node that was away gets the entries it missed for up to 6 hours, while the leader's disk has room; after that it gets a snapshot, only the part it lacks, exits with code 75 and loads it at start.",
  draw(c) {
    const rows = [
      ["heartbeat", "every 100 ms", "plain"],
      ["election", "no word for 1–2 s: followers vote a new leader", "plain"],
      ["leader loses its majority", "1.5 s: new writes wait, in order · 3 s: it hands leadership away", "danger"],
      ["a node comes back", "≤ 6 h away: the entries it missed · longer: a snapshot of what it lacks, exit 75", "accent"],
    ];
    rows.forEach(([what, how, tone], i) => {
      const y = 24 + i * 66;
      c.box(20, y, 260, 50, what, { size: 15.5, tone });
      c.text(300, y + 25, how, { size: 14.5 });
    });
    c.text(500, 288, "The nodes talk on port 7400: plain HTTP, with the shared token.", { size: 14, anchor: "middle", tone: "muted", italic: true });
  },
};
