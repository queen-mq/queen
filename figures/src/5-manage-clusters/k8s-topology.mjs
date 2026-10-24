// Chapter 5.2: the shape of the cluster on Kubernetes.
export default {
  id: "k8s-topology",
  height: 360,
  alt: "Namespace queen. A StatefulSet runs three pods, queen-0, queen-1 and queen-2, which are nodes 1, 2 and 3, each on its own machine because of the required anti-affinity, each with its own volume claim. A headless Service lets the pods find each other by name on ports 6632 and 7400, also before they are ready. The client Service queen carries port 6632 to any ready pod. A PodDisruptionBudget lets Kubernetes evict one pod at a time.",
  draw(c) {
    c.frame(20, 30, 960, 310, "namespace queen", { tone: "ghost" });
    c.box(60, 58, 260, 50, "Service queen :6632", { tone: "info", size: 15, sub: "clients, any ready pod" });
    c.box(370, 58, 300, 50, "headless Service", { size: 15, sub: "pods find each other: 6632, 7400" });
    c.box(720, 58, 220, 50, "PodDisruptionBudget", { size: 14.5, sub: "maxUnavailable: 1" });
    const pods = [["queen-0", "node 1"], ["queen-1", "node 2"], ["queen-2", "node 3"]];
    pods.forEach(([p, n], i) => {
      const x = 70 + i * 300;
      c.frame(x - 10, 150, 270, 170, `machine ${i + 1}`, { tone: "ghost" });
      c.box(x + 10, 172, 230, 66, p, { sub: n, tone: i === 0 ? "accent" : "strong", mono: true });
      c.disk(x + 55, 250, 140, 58, `data-${p}`, { size: 13, mono: true });
    });
    c.text(500, 130, "StatefulSet queen: 3 replicas, one per machine (required anti-affinity)", { size: 13.5, anchor: "middle", tone: "muted" });
  },
};
