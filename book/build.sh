#!/usr/bin/env bash
# Build "The little book of QueenMQ" as a PDF.
#
#   book/build.sh            -> book/build/the-little-book-of-queenmq.pdf
#   book/build.sh --publish  -> also puts this edition on the website: the PDF, its cover
#                               and edition.json in webdoc/public/book/, which the page
#                               /book/ (webdoc/src/content/docs/book.mdx) shows. Commit them.
#
# The book is LaTeX: book/main.tex (cover, parts, the order of the chapters),
# book/chapters/*.tex (the text) and book/latex/queen.sty (the look). This script
# prepares what the LaTeX reads from build/, then compiles it with Tectonic.
#
# Needs tectonic, pandoc, rsvg-convert, ImageMagick (brew install tectonic pandoc librsvg
# imagemagick) and node; --publish also needs poppler (pdfinfo, pdftoppm).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
OUT="$HERE/build"
rm -rf "$OUT/code"
mkdir -p "$OUT/assets" "$OUT/code"

# 1. The code of \guide{<step>}: each step of the JavaScript guide program, highlighted
#    by pandoc in the site's colours (latex/queen.theme). This is the code that runs.
node "$HERE/latex/code.mjs" "$OUT/code"
for f in "$OUT"/code/*.md; do
  pandoc "$f" --to latex --syntax-highlighting="$HERE/latex/queen.theme" --output "${f%.md}.tex.body"
  # \codelines{n} first: a short box (see latex/queen.sty) is kept whole on one page.
  { printf '\\codelines{%d}\n' "$(($(wc -l < "$f") - 2))"; cat "${f%.md}.tex.body"; } > "${f%.md}.tex"
  rm "${f%.md}.tex.body"
done
# ...and the macros those fragments use (the token colours), from a sample document.
printf '```js\nconst a = 1\n```\n' \
  | pandoc --standalone --to latex --syntax-highlighting="$HERE/latex/queen.theme" \
  | grep -E '^\\(newcommand\{\\VerbBar\}|newcommand\{\\VERB\}|DefineVerbatimEnvironment\{Highlighting\}|definecolor\{shadecolor\}|newenvironment\{Shaded\}|newcommand\{\\[A-Za-z]+Tok\})' \
  > "$OUT/highlighting.tex"

# Every \guide{...} a chapter asks for has to exist.
missing=0
while read -r step; do
  if [ ! -f "$OUT/code/$step.tex" ]; then
    echo "error: \\guide{$step} has no guide:start($step) region in examples/tutorials/js/00-five-minutes.mjs or book-client-guide.mjs" >&2
    missing=1
  fi
done < <(grep -rhoE '\\guide\{[a-z-]+\}' "$HERE/chapters" 2>/dev/null | sed 's/\\guide{\(.*\)}/\1/' | sort -u)
[ "$missing" = 0 ] || exit 1

# 2. The cover art: queenmq.cloud's sunflower and bee. book/assets/queen-scene.svg is,
#    byte for byte, queen-cloud's webapp/src/trial/queen-scene.svg (the reverted
#    homepage hero of 4d7b9107f). It names no colour and it animates; rsvg-convert
#    reads neither CSS variables nor animations, so the dark page's colours are
#    written in and the drawing is its resting frame: the whole flower, the bee on a
#    petal. It goes in as a transparent PNG at about 530 dpi, because the seeds are
#    cut out with SVG masks, which a PDF carries as soft masks that Apple's renderer
#    (Preview, Quick Look) ignores.
sed -e 's/var(--scene-ink)/#f2f0ec/g' -e 's/var(--scene-paper)/#0b0a0a/g' \
    -e 's/var(--scene-yellow)/#fcc620/g' "$HERE/assets/queen-scene.svg" > "$OUT/assets/queen-scene-dark.svg"
rsvg-convert -h 3800 -o "$OUT/assets/scene-on-dark.png" "$OUT/assets/queen-scene-dark.svg"
rsvg-convert -f pdf -o "$OUT/assets/wordmark-on-dark.pdf" "$ROOT/assets/logo/queen-on-dark.svg"

# Screenshots come from the site's own files (WebP there; LaTeX reads PNG), so the
# book always shows the dashboard the homepage shows. Light mode, for a white page.
magick "$ROOT/webdoc/public/screenshots/dashboard-overview-light.webp" "$OUT/assets/dashboard-overview.png"

# The drawings: figures/src in the repository, one source for the book and the website.
node "$ROOT/figures/build.mjs" --pdf "$OUT/figures" >/dev/null
missing=0
while read -r id; do
  if [ ! -f "$OUT/figures/$id.pdf" ]; then
    echo "book: \\queenfig{$id} names no figure in figures/src" >&2
    missing=1
  fi
done < <(grep -rhoE '\\queenfig(\[[a-z!]*\])?\{[a-z0-9-]+\}' "$HERE/chapters" 2>/dev/null | sed -E 's/.*\{(.*)\}/\1/' | sort -u)
[ "$missing" = 0 ] || exit 1

# 3. The date on the cover.
printf '\\newcommand{\\builddate}{%s}\n' "$(date '+%B %Y')" > "$OUT/meta.tex"

# 4. Compile.
(cd "$HERE" && tectonic --chatter minimal --outdir build main.tex)
mv "$OUT/main.pdf" "$OUT/the-little-book-of-queenmq.pdf"
echo "wrote $OUT/the-little-book-of-queenmq.pdf"

# 5. With --publish, this edition goes on the website. Only on purpose: a draft built
#    while writing must not replace the edition readers download.
if [ "${1:-}" = "--publish" ]; then
  SITE="$ROOT/webdoc/public/book"
  mkdir -p "$SITE"
  cp "$OUT/the-little-book-of-queenmq.pdf" "$SITE/the-little-book-of-queenmq.pdf"
  # The cover, as the page shows it: page 1 at 110 dpi, about 770 px wide.
  pdftoppm -f 1 -l 1 -r 110 -png "$OUT/the-little-book-of-queenmq.pdf" "$OUT/cover"
  magick "$OUT"/cover-*1.png -quality 82 "$SITE/cover.webp"
  rm -f "$OUT"/cover-*.png
  pages=$(pdfinfo "$OUT/the-little-book-of-queenmq.pdf" | awk '/^Pages:/ {print $2}')
  bytes=$(wc -c < "$OUT/the-little-book-of-queenmq.pdf" | tr -d ' ')
  version=$(node -p "require('$ROOT/server/server.json').version")
  read -r cw ch < <(magick identify -format '%w %h\n' "$SITE/cover.webp")
  printf '{\n  "date": "%s",\n  "edition": "%s",\n  "pages": %s,\n  "bytes": %s,\n  "queen": "%s",\n  "cover": [%s, %s]\n}\n' \
    "$(date '+%Y-%m-%d')" "$(date '+%B %Y')" "$pages" "$bytes" "$version" "$cw" "$ch" > "$SITE/edition.json"
  echo "published to $SITE: $pages pages, $bytes bytes, QueenMQ $version"
fi
