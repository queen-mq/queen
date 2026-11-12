"""Build the talk into the site: public/talks/coming-from-another-broker/.

Run: python3 webdoc/talks/coming-from-another-broker/build.py
It writes index.html, the slides' script (deck.js), the script that sets the
theme before the first paint (theme.js) and the two fonts. The site serves
public/ as it is, under its security policy (public/_headers): scripts come
from files on the site, never inline, and the fonts are the site's own, not
Google's.
The wordmark and the scene come from the site itself (src/assets, public),
so a new logo or illustration reaches the talk on the next build.
"""

import shutil
from pathlib import Path

here = Path(__file__).resolve().parent
webdoc = here.parent.parent
src = here / "src"
out = webdoc / "public" / "talks" / here.name

# family: (fontsource package, file, weights): the latin cut of the site's two variable fonts
FONTS = {
    "Inter Variable": ("inter", "inter-latin-wght-normal.woff2", "100 900"),
    "JetBrains Mono Variable": ("jetbrains-mono", "jetbrains-mono-latin-wght-normal.woff2", "100 800"),
}


def read(name):
    return (src / name).read_text()


chapters = [f"ch{i}" for i in range(12) if (src / f"ch{i}.html").exists()]

(out / "fonts").mkdir(parents=True, exist_ok=True)
faces = ""
for family, (package, file, weights) in FONTS.items():
    shutil.copyfile(webdoc / "node_modules/@fontsource-variable" / package / "files" / file, out / "fonts" / file)
    faces += (f'@font-face {{ font-family: "{family}"; font-style: normal; font-display: swap; '
              f'font-weight: {weights}; src: url("fonts/{file}") format("woff2"); }}\n')

html = read("head.html").replace("__CSS__", faces + read("style.css"))
html += "".join(read(c + ".html") for c in chapters)
html += read("foot.html")
html += '<script src="deck.js"></script>\n</body>\n</html>\n'
html = html.replace("__WORDMARK__", (webdoc / "src/assets/queen.svg").read_text().strip())
html = html.replace("__PUBLIC__", "")
js = read("core.js") + "".join(read(c + ".js") for c in chapters if (src / f"{c}.js").exists())

(out / "index.html").write_text(html)
(out / "deck.js").write_text("(() => {\n" + js + "})();\n")
shutil.copyfile(src / "theme.js", out / "theme.js")
print("built", len(chapters), "chapters,", len(html), "bytes ->", out.relative_to(webdoc))
