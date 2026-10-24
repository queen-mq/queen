// Chapter 3.9: a timer, keyed: schedule, move, cancel, fire.
export default {
  id: "timer-reminder",
  height: 300,
  alt: "A timeline for the reminder review-2001 in the queue review-reminders. Scheduled at delivery for 24 hours later: status scheduled. The delivery is late, so the same key is scheduled again for 48 hours: status rescheduled, and the first time is gone. The guest writes a review, so the key is cancelled. A timer that is not cancelled fires as a normal message of its queue, through the log.",
  draw(c) {
    const x0 = 60, x1 = 960;
    c.axis(x0, x1, 200, [], { label: "time" });
    const marks = [
      [120, "schedule +24 h", "scheduled", "ok"],
      [400, "schedule again +48 h", "rescheduled", "accent"],
      [700, "cancel", "removed", "danger"],
    ];
    marks.forEach(([x, what, status, tone]) => {
      c.line(x, 120, x, 200, { tone: "line", dash: "4 4" });
      c.text(x, 92, what, { size: 14.5, anchor: "middle", weight: 600 });
      c.tag(x, 120, status, { tone, anchor: "middle", mono: true, size: 13.5 });
    });
    c.rect(120, 170, 280, 14, { tone: "muted", r: 4 });
    c.text(260, 160, "first due time, replaced", { size: 12.5, anchor: "middle", tone: "faint" });
    c.rect(400, 170, 300, 14, { tone: "accent", r: 4 });
    c.text(20, 40, "review-reminders / review-2001", { size: 15, mono: true, weight: 600 });
    c.text(500, 250, "Not cancelled, a timer fires as a normal message of its queue, through the log.", { size: 14, anchor: "middle", tone: "muted", italic: true });
    c.text(500, 274, "A cancel can arrive just after it fired: check the order's state before sending.", { size: 14, anchor: "middle", tone: "danger", italic: true });
  },
};
