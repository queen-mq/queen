#!/usr/bin/env python3
"""Generate the homepage's animated sunflower and queue consumer.

The petals reuse a petal from Alberto's logo; the drawing keeps its flat yellow
and ink palette. The SVG works both as a standalone image and in QueenScene.astro,
where theme tokens, the pause control and visibility govern its CSS animations.
Run: python3 assets/scene.py
"""
import math
from pathlib import Path
import re

import logo as L

ROOT = Path(__file__).resolve().parent.parent
CENTRE = (485, 400)


def partition_seeds():
    """Preserve the original 233-partition disc, including the seed sizes."""
    head = 94
    step = 0.86 * head / math.sqrt(233)
    points = []
    for i in range(233):
        radius = step * math.sqrt(i + 0.5)
        angle = math.radians(i * 137.50776405003785)
        size = 0.46 * step * (1 - 0.35 + 2 * 0.35 * radius / (0.86 * head))
        points.append((radius * math.cos(angle), radius * math.sin(angle), size))
    return points


SEEDS = partition_seeds()
# One worker in one consumer group, visiting three partitions of ONE queue.
# A seed is a partition, never a message: it stays after the ack. The small
# pollen dots depict delivery of a batch, not removal of its retained records.
VISITS = [
    {"partition": "customer-42", "offset": 42, "near": (-57, -40), "bee": (365, 347), "mouth": (399, 355)},
    {"partition": "customer-17", "offset": 108, "near": (-68, 12), "bee": (352, 414), "mouth": (386, 414)},
    {"partition": "customer-89", "offset": 7, "near": (-28, 61), "bee": (390, 485), "mouth": (424, 476)},
]
for visit in VISITS:
    index = min(range(len(SEEDS)), key=lambda i: math.dist(SEEDS[i][:2], visit["near"]))
    visit["seed"] = index
    visit["point"] = (CENTRE[0] + SEEDS[index][0], CENTRE[1] + SEEDS[index][1])


def flower():
    # The logo's top petal, centred on its base and scaled into a full corolla.
    petal = re.findall(r"M[^M]+", L.PETALS)[-1]
    petals = "".join(
        f'<g transform="rotate({i * 30 + 5})"><g transform="translate(0,-104) scale(2.65) translate(-113,-183)">'
        f'<path d="{petal}"/></g></g>' for i in range(12)
    )
    seeds = []
    for i, (x, y, radius) in enumerate(SEEDS):
        visit = next((j for j, v in enumerate(VISITS) if v["seed"] == i), None)
        active = '' if visit is None else f' class="partition-seed" style="--visit-delay:{visit * 6}s"'
        seeds.append(f'<circle data-partition="{i}" cx="{x:.2f}" cy="{y:.2f}" r="{radius:.2f}"{active}/>')
    return (f'<g transform="translate({CENTRE[0]},{CENTRE[1]})"><g fill="{L.YELLOW}">{petals}</g>'
            '<circle r="94" class="ink-fill"/>'
            f'<g class="paper-fill">{"".join(seeds)}</g>'
            '<circle r="87.42" fill="none" class="paper-stroke" stroke-width="1.88"/></g>')


def bee():
    # Mirror the bee so she faces the flower; scale within the flight frame.
    return f'''<g class="consumer-flight"><g class="consumer-hover"><g transform="scale(-0.78,0.78)">
      <g class="bee-wing bee-wing-far">
        <path d="M7 -15 C-11 -30 -21 -77 -2 -78 C20 -79 34 -40 19 -15 Z"/>
      </g>
      <path d="M56 8 L69 13 L56 18 Z" class="ink-fill"/>
      <g clip-path="url(#queen-bee-body)">
        <ellipse cx="22" cy="10" rx="39" ry="29" fill="{L.YELLOW}"/>
        <path d="M5 -20 Q-8 10 5 40 M29 -20 Q16 10 29 40 M54 -16 Q42 12 54 37"
          fill="none" class="ink-stroke" stroke-width="12"/>
      </g>
      <ellipse cx="22" cy="10" rx="39" ry="29" fill="none" class="ink-stroke" stroke-width="3"/>
      <path d="M-5 31 Q-9 44 -20 41 M15 37 Q14 47 3 46" class="ink-stroke"
        fill="none" stroke-width="3.5" stroke-linecap="round"/>
      <g class="bee-wing bee-wing-near">
        <path d="M8 -12 C-21 -17 -48 -55 -29 -67 C-8 -80 22 -37 20 -14 Z"/>
        <path d="M11 -20 Q-2 -43 -25 -59" fill="none" class="wing-vein"/>
      </g>
      <circle cx="-20" cy="6" r="24" class="ink-fill"/>
      <path d="M-36 -11 Q-46 -24 -53 -21" fill="none" class="ink-stroke" stroke-width="3.5" stroke-linecap="round"/>
      <circle cx="-54" cy="-21" r="3.5" class="ink-fill"/>
      <ellipse cx="-29" cy="3" rx="5.5" ry="7" class="paper-fill"/>
      <circle cx="-31" cy="3" r="2.6" class="ink-fill"/>
      <path d="M-36 16 Q-31 20 -26 16" fill="none" class="paper-stroke" stroke-width="2" stroke-linecap="round"/>
      <g transform="translate(-22,-20) rotate(-8)" fill="{L.YELLOW}">
        <path d="M-14 0 L-17 -19 L-7 -12 L0 -25 L7 -12 L17 -19 L14 0 Z"/>
        <circle cx="-17" cy="-20" r="2.5"/><circle cy="-26" r="2.5"/><circle cx="17" cy="-20" r="2.5"/>
      </g>
    </g></g></g>'''


def partition_activity():
    out = []
    for i, visit in enumerate(VISITS):
        x, y = visit["point"]
        mx, my = visit["mouth"]
        out.append(f'''<g class="partition-visit" data-seed="{visit['seed']}" data-partition-key="{visit['partition']}" data-first-offset="{visit['offset']}" style="--visit-delay:{i * 6}s">
          <g transform="translate({x:.2f},{y:.2f})">
            <circle r="11" class="partition-lease"/>
            <circle r="11" class="partition-ack"/>
          </g>
          <path d="M{x:.2f} {y:.2f} Q{mx + 18:.2f} {my - 13:.2f} {mx} {my}" class="delivery-path"/>
        ''')
        for n in range(3):
            out.append(f'''<circle cx="{x:.2f}" cy="{y:.2f}" r="3" class="pollen"
              style="--pollen-delay:{i * 6 + n * 0.18}s; --read-x:{mx - x:.2f}px; --read-y:{my - y:.2f}px"/>''')
        out.append('</g>')
    return "".join(out)



CSS = '''
.queen-illustration { --scene-ink: var(--nb-foreground, INK); --scene-paper: var(--nb-background, PAPER); --scene-yellow: #fcc620; --zoom-y: -530px; color: var(--scene-ink); }
@media (max-width: 1279px), (max-height: 900px) { .queen-illustration { --zoom-y: -680px; } }
.queen-illustration .scene-camera { transform-origin: 0 0; animation: queen-camera 18s ease-in-out infinite; }
.queen-illustration .ink-fill { fill: var(--scene-ink); }
.queen-illustration .ink-stroke { stroke: var(--scene-ink); }
.queen-illustration .paper-fill { fill: var(--scene-paper); }
.queen-illustration .paper-stroke { stroke: var(--scene-paper); }
.queen-illustration .partition-seed { animation: queen-seed 18s infinite; animation-delay: var(--visit-delay); }
.queen-illustration .partition-lease { fill: var(--scene-yellow); fill-opacity: .14; stroke: var(--scene-yellow); stroke-width: 1.5; opacity: 0; animation: queen-lease 18s infinite; }
.queen-illustration .partition-ack { fill: none; stroke: var(--scene-yellow); stroke-width: 2; opacity: 0; animation: queen-ack 18s infinite; }
.queen-illustration .delivery-path { fill: none; stroke: var(--scene-yellow); stroke-width: 1.5; stroke-dasharray: 2 5; opacity: 0; animation: queen-delivery 18s infinite; }
.queen-illustration .pollen { fill: var(--scene-yellow); opacity: 0; animation: queen-pollen 18s ease-in-out infinite; animation-delay: var(--pollen-delay); }
.queen-illustration .partition-lease, .queen-illustration .partition-ack, .queen-illustration .delivery-path { animation-delay: var(--visit-delay); }
.queen-illustration .consumer-flight { transform: translate(365px,347px) rotate(10deg); animation: queen-route 18s infinite; }
.queen-illustration .consumer-hover { animation: queen-hover 1.6s ease-in-out infinite; }
.queen-illustration .bee-wing { fill: var(--scene-paper); stroke: var(--scene-ink); stroke-width: 2.5; transform-origin: 12px -14px; }
.queen-illustration .bee-wing-far { animation: queen-wing-far .19s ease-in-out infinite alternate; }
.queen-illustration .bee-wing-near { animation: queen-wing-near .16s ease-in-out infinite alternate; }
.queen-illustration .wing-vein { stroke: var(--scene-ink); stroke-opacity: .2; stroke-width: 1.5; }
.queen-illustration .flight-trail { fill: none; stroke: currentColor; stroke-width: 1.5; stroke-opacity: .15; stroke-linecap: round; stroke-dasharray: 2 9; }
@keyframes queen-camera {
  0%, 4%, 97%, 100% { transform: translate(0,0) scale(1); }
  12%, 90% { transform: translate(-594px,var(--zoom-y)) scale(2.25); }
}
[data-camera-view="detail"] .queen-illustration .scene-camera { transform: translate(-594px,var(--zoom-y)) scale(2.25) !important; }
[data-camera-view="full"] .queen-illustration .scene-camera { transform: translate(0,0) scale(1) !important; }
@keyframes queen-route {
  0%, 25% { transform: translate(365px,347px) rotate(10deg); }
  29% { transform: translate(326px,377px) rotate(2deg); }
  33.333%, 58.333% { transform: translate(352px,414px) rotate(0deg); }
  62% { transform: translate(346px,469px) rotate(-12deg); }
  66.666%, 91.666% { transform: translate(390px,485px) rotate(-15deg); }
  94% { transform: translate(279px,464px) rotate(-25deg); }
  97% { transform: translate(283px,295px) rotate(-8deg); }
  100% { transform: translate(365px,347px) rotate(10deg); }
}
@keyframes queen-hover { 0%, 100% { transform: translateY(0); } 50% { transform: translateY(-3px); } }
@keyframes queen-wing-near { from { transform: rotate(-9deg) scaleY(.92); } to { transform: rotate(12deg) scaleY(.65); } }
@keyframes queen-wing-far { from { transform: rotate(8deg) scaleX(.85); } to { transform: rotate(-12deg) scaleX(.6); } }
@keyframes queen-seed { 0%, 26% { fill: var(--scene-yellow); } 30%, 100% { fill: var(--scene-paper); } }
@keyframes queen-lease { 0%, 23% { opacity: 1; } 27%, 100% { opacity: 0; } }
@keyframes queen-delivery { 0%, 2%, 13%, 100% { opacity: 0; } 4%, 10% { opacity: .8; } }
@keyframes queen-pollen {
  0%, 3% { opacity: 0; transform: translate(0,0); }
  4% { opacity: 1; transform: translate(0,0); }
  10% { opacity: 1; transform: translate(var(--read-x),var(--read-y)); }
  12%, 100% { opacity: 0; transform: translate(var(--read-x),var(--read-y)); }
}
@keyframes queen-ack {
  0%, 18% { opacity: 0; transform: scale(.7); }
  19% { opacity: 1; transform: scale(.7); }
  26% { opacity: 0; transform: scale(2.5); }
  100% { opacity: 0; transform: scale(2.5); }
}
[data-animation-paused] .queen-illustration * { animation-play-state: paused !important; }
@media (prefers-reduced-motion: reduce) { .queen-illustration * { animation-play-state: paused; } }
[data-animation-enabled] .queen-illustration * { animation-play-state: running; }
'''


def scene(ink, paper):
    css = CSS.replace("INK", ink).replace("PAPER", paper)
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 1080" width="800" height="1080"
      class="queen-illustration" role="img" aria-labelledby="queen-scene-title queen-scene-desc">
      <title id="queen-scene-title">One queue, many partitions, one busy worker.</title>
      <desc id="queen-scene-desc">The sunflower represents the orders queue. Its 233 seeds represent partitions. One worker in a consumer group visits three partitions: it leases a batch, processes its messages in order, then acknowledges them. An ack advances that group’s cursor; the partition and retained messages remain. This illustration shows successful consumption, not retries or a fixed scheduling order.</desc>
      <style>{css}</style>
      <defs><clipPath id="queen-bee-body"><ellipse cx="22" cy="10" rx="39" ry="29"/></clipPath></defs>
      <g class="scene-camera">
      <path d="M485 482 C490 685 422 838 433 1100" fill="none" class="ink-stroke" stroke-width="15" stroke-linecap="round"/>
      <path d="M455 802 C381 810 309 768 302 695 C381 695 442 735 455 802 Z" class="ink-fill"/>
      <path d="M432 940 C507 949 577 901 587 833 C511 828 445 871 432 940 Z" class="ink-fill"/>
      <path d="M448 791 Q380 749 320 711 M441 927 Q507 879 567 846" fill="none" class="paper-stroke" stroke-width="3" stroke-linecap="round"/>
      {flower()}
      <path d="M360 517 C198 477 223 258 334 306" class="flight-trail"/>
      {partition_activity()}
      {bee()}
      </g>
    </svg>
'''


def main():
    for name, ink, paper in (("on-light", L.INK, "#f2f0ec"), ("on-dark", L.PAPER, "#0b0a0a")):
        target = ROOT / "webdoc" / "public" / f"queen-scene-{name}.svg"
        target.write_text(scene(ink, paper))
        print(target.relative_to(ROOT))


if __name__ == "__main__":
    main()
