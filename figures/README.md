# Figures

The drawings of the little book of QueenMQ and of the website: one source for both.

- `src/<chapter>/<id>.mjs` — one figure each: `{ id, height, alt, draw(canvas) }`. Every figure is
  1000 units wide, so all of them print at the same type size.
- `lib/draw.mjs` — the drawing kit (boxes, arrows, partition strips, cursors, timelines, sequence
  diagrams), with a light and a dark palette.
- `build.mjs` — writes `out/<id>.svg` (light) and `out/<id>.dark.svg`, plus `out/index.json`;
  `--pdf <dir>` also renders PDFs for the book with rsvg-convert, `--png <dir>` previews, `--site <dir>`
  copies the SVGs for the website (webdoc runs it before every build).
- `fonts/` — Inter and Source Code Pro (SIL Open Font License 1.1), the book's own faces, so
  rsvg-convert sets the figures in them.

```bash
node figures/build.mjs --png /tmp/figures      # look at every figure
```
