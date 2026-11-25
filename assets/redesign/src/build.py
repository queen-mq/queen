"""Sheets for her: her 18.png next to the two custom-letter directions, same x-height;
night versions; one colour; real sizes. Also the SVGs for Affinity."""
import io, subprocess
from PIL import Image, ImageDraw, ImageFont
from letters import lockup, WHITE, NIGHT

XPX = 120                       # x-height on the sheets, px
HERS = "../../images/18.png"     # her Arial Black version, x-height 138 px
FONT = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial.ttf", 22)
GREY = (120, 120, 120)


def png(svg, width=None):
    cmd = ["rsvg-convert"] + (["-w", str(width)] if width else [])
    im = Image.open(io.BytesIO(subprocess.run(cmd, input=svg.encode(), capture_output=True, check=True).stdout)).convert("RGBA")
    from collections import Counter                                       # ground = commonest opaque colour on the border
    border = [im.getpixel((x, y)) for x in range(im.width) for y in (0, 1, im.height - 2)] + \
             [im.getpixel((x, y)) for y in range(im.height) for x in (0, 1, im.width - 2)]
    opaque = Counter(px[:3] for px in border if px[3] == 255)
    ground = opaque.most_common(1)[0][0] if opaque else (255, 255, 255)
    bg = Image.new("RGBA", im.size, ground + (255,))                      # flatten on the ground colour
    return Image.alpha_composite(bg, im).convert("RGB")


def hers(scale=1.0, pad=0.5):
    im = Image.open(HERS).convert("RGB")
    p = round(pad * 138)
    box = (max(61 - p, 0), max(55 - p, 0), min(1593 + p, im.width), min(399 + p, im.height))
    im = im.crop(box)
    return im.resize((round(im.width * scale), round(im.height * scale)), Image.LANCZOS)


def stack(rows, bg=(255, 255, 255), gap=26, label_c=GREY, rule=(230, 230, 230)):
    W = max(im.width for _, im in rows) + 2 * gap
    H = sum(im.height + 40 for _, im in rows) + gap * (len(rows) + 1)
    sheet = Image.new("RGB", (W, H), bg)
    dr = ImageDraw.Draw(sheet)
    y = gap
    for i, (label, im) in enumerate(rows):
        dr.text((gap, y), label, fill=label_c, font=FONT)
        y += 40
        sheet.paste(im, (gap + (W - 2 * gap - im.width) // 2, y))
        y += im.height + gap
        if i < len(rows) - 1:
            dr.line([(gap, y - gap // 2), (W - gap, y - gap // 2)], fill=rule, width=2)
    return sheet


if __name__ == "__main__":
    s = XPX / 100
    # SVGs for Affinity (x-height 160 px)
    for which, nm in (("D1", "one-pipe"), ("D2", "black-round")):
        lockup(which, name=f"queenmq-{nm}")
        lockup(which, ground=NIGHT, name=f"queenmq-{nm}-night")
        lockup(which, mono=True, name=f"queenmq-{nm}-one-colour")
    # 1. white: hers, one pipe, black round, same x-height
    rows = [("yours today: Arial Black", hers(XPX / 138)),
            ("1  one pipe: letters and stem are one pipe", png(lockup("D1", scale=s)[0])),
            ("2  black round: Black weight from circles, thin stem", png(lockup("D2", scale=s)[0]))]
    stack(rows).save("out/queenmq-custom-overview.png")
    # 2. night
    rows = [("1  one pipe", png(lockup("D1", ground=NIGHT, scale=s)[0])),
            ("2  black round", png(lockup("D2", ground=NIGHT, scale=s)[0]))]
    stack(rows, bg=(21, 19, 17), label_c=(150, 145, 138), rule=(45, 42, 38)).save("out/queenmq-custom-night.png")
    # 3. real sizes, word width 360 / 220 / 140 px, plus one colour at 140
    gap = 30
    cells = []
    for label, make in (("yours", None), ("1  one pipe", "D1"), ("2  black round", "D2")):
        row = []
        for w in (360, 220, 140):
            if make is None:
                im = hers(1.0, pad=0.12)
                im = im.resize((w, round(im.height * w / im.width)), Image.LANCZOS)
            else:
                im = png(lockup(make, pad=0.12)[0], width=w)
            row.append(im)
        if make:
            row.append(png(lockup(make, mono=True, pad=0.12)[0], width=140))
        cells.append((label, row))
    W = gap + sum(im.width + gap for im in cells[1][1]) + 60
    H = gap + sum(max(im.height for im in r) + 40 + gap for _, r in cells)
    sheet = Image.new("RGB", (W, H), (255, 255, 255))
    dr = ImageDraw.Draw(sheet)
    y = gap
    for label, row in cells:
        dr.text((gap, y), label, fill=GREY, font=FONT)
        y += 40
        x = gap
        for im in row:
            sheet.paste(im, (x, y + (max(i.height for i in row) - im.height)))
            x += im.width + gap
        y += max(i.height for i in row) + gap
    sheet.save("out/queenmq-custom-sizes.png")
    print("ok")
