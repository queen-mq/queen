// BM25 over a few thousand short documents. The corpus is small enough that a
// plain keyword index answers in well under a millisecond: no embeddings, no
// vector store.

const STOP = new Set(
  "a an and are as at be but by can do does for from has have how i if in into is it its me my of on or so than that the their them then there these this to was what when where which who why will with you your".split(
    " ",
  ),
);

export function tokens(text) {
  const out = [];
  for (const raw of String(text).toLowerCase().match(/[a-z0-9_]+/g) ?? []) {
    const parts = raw.includes("_") ? [raw, ...raw.split("_").filter(Boolean)] : [raw];
    for (let t of parts) {
      if (t.length < 2 || STOP.has(t)) continue;
      if (t.length > 4 && t.endsWith("ies")) t = `${t.slice(0, -3)}y`;
      else if (t.length > 3 && t.endsWith("s") && !t.endsWith("ss")) t = t.slice(0, -1);
      out.push(t);
    }
  }
  return out;
}

export class Index {
  /** @param fields (doc) => [{ text, weight }] */
  constructor(docs, fields) {
    this.docs = docs;
    this.tf = [];
    this.len = [];
    this.df = new Map();
    let total = 0;
    for (const doc of docs) {
      const tf = new Map();
      let len = 0;
      for (const { text, weight } of fields(doc)) {
        for (const t of tokens(text)) {
          tf.set(t, (tf.get(t) ?? 0) + weight);
          len += weight;
        }
      }
      this.tf.push(tf);
      this.len.push(len);
      total += len;
      for (const t of tf.keys()) this.df.set(t, (this.df.get(t) ?? 0) + 1);
    }
    this.avg = total / Math.max(docs.length, 1);
  }

  search(query, limit = 5, keep = null) {
    const terms = [...new Set(tokens(query))];
    const n = this.docs.length;
    const k1 = 1.2;
    const b = 0.75;
    const hits = [];
    this.docs.forEach((doc, i) => {
      if (keep && !keep(doc)) return;
      const tf = this.tf[i];
      let score = 0;
      for (const t of terms) {
        const f = tf.get(t);
        if (!f) continue;
        const df = this.df.get(t);
        const idf = Math.log(1 + (n - df + 0.5) / (df + 0.5));
        score += (idf * f * (k1 + 1)) / (f + k1 * (1 - b + (b * this.len[i]) / this.avg));
      }
      if (score > 0) hits.push({ doc, score });
    });
    return hits.sort((x, y) => y.score - x.score).slice(0, limit);
  }
}
