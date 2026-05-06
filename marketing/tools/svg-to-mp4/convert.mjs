// Convert an animated SMIL SVG into a LinkedIn-ready MP4.
//
// Output spec (matches LinkedIn's autoplay requirements):
//   1920x1080, H.264, yuv420p, +faststart, ~10 Mbps, no audio.
// SVGs from docs/assets/ are 1600x760 (ratio 2.105:1).
// We render onto a 1920x912 inner viewport, then pad to 1920x1080
// (16:9) with the page background colour, so LinkedIn's desktop feed
// shows it without letterboxing artifacts.

import { mkdir, readFile, writeFile, rm } from 'node:fs/promises'
import { existsSync } from 'node:fs'
import { spawn } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import puppeteer from 'puppeteer'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const REPO_ROOT = path.resolve(__dirname, '../../..')

const args = parseArgs(process.argv.slice(2))
const svgPath = path.resolve(args.svg ?? path.join(REPO_ROOT, 'docs/assets/queen-vs-kafka.svg'))
const outPath = path.resolve(args.out ?? path.join(REPO_ROOT, 'marketing/assets/queen-vs-kafka.mp4'))
const duration = Number(args.duration ?? 12) // seconds
const fps = Number(args.fps ?? 30)
const innerW = 1920
const innerH = 912
const finalW = 1920
const finalH = 1080
const padTop = Math.floor((finalH - innerH) / 2)
const bgColour = '#141415' // matches docs/style.css --ink-0

const framesDir = path.join(__dirname, '.frames')

console.log(`[svg-to-mp4] svg:      ${svgPath}`)
console.log(`[svg-to-mp4] out:      ${outPath}`)
console.log(`[svg-to-mp4] duration: ${duration}s @ ${fps}fps (${duration * fps} frames)`)
console.log(`[svg-to-mp4] render:   ${innerW}x${innerH} -> pad ${finalW}x${finalH}`)

await rm(framesDir, { recursive: true, force: true })
await mkdir(framesDir, { recursive: true })
await mkdir(path.dirname(outPath), { recursive: true })

const svgContent = await readFile(svgPath, 'utf-8')
const html = `<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>
  html, body { margin: 0; padding: 0; background: ${bgColour}; }
  body { width: ${innerW}px; height: ${innerH}px; overflow: hidden; }
  svg { width: 100%; height: 100%; display: block; }
  /* Force animations to start from a consistent baseline */
  * { animation-delay: 0s !important; }
</style></head>
<body>${svgContent}</body></html>`

const browser = await puppeteer.launch({
  headless: 'new',
  args: ['--no-sandbox', '--disable-dev-shm-usage', '--font-render-hinting=none'],
  defaultViewport: { width: innerW, height: innerH, deviceScaleFactor: 1 }
})

try {
  const page = await browser.newPage()
  await page.setContent(html, { waitUntil: 'networkidle0' })
  // Give SMIL one frame to settle.
  await new Promise(r => setTimeout(r, 200))

  const totalFrames = duration * fps
  const frameInterval = 1000 / fps // ms

  console.log(`[svg-to-mp4] capturing ${totalFrames} frames...`)
  const t0 = Date.now()

  for (let i = 0; i < totalFrames; i++) {
    const target = t0 + i * frameInterval
    const wait = target - Date.now()
    if (wait > 0) await new Promise(r => setTimeout(r, wait))

    const buf = await page.screenshot({
      type: 'png',
      omitBackground: false,
      clip: { x: 0, y: 0, width: innerW, height: innerH }
    })
    await writeFile(path.join(framesDir, `f_${String(i).padStart(5, '0')}.png`), buf)

    if (i % 30 === 0) {
      process.stdout.write(`\r  frame ${i}/${totalFrames}`)
    }
  }
  process.stdout.write(`\r  frame ${totalFrames}/${totalFrames}\n`)
} finally {
  await browser.close()
}

console.log(`[svg-to-mp4] encoding MP4 with ffmpeg...`)
await runFfmpeg([
  '-y',
  '-framerate', String(fps),
  '-i', path.join(framesDir, 'f_%05d.png'),
  '-vf', `pad=${finalW}:${finalH}:0:${padTop}:0x141415FF,format=yuv420p`,
  '-c:v', 'libx264',
  '-preset', 'slow',
  '-crf', '18',
  '-pix_fmt', 'yuv420p',
  '-movflags', '+faststart',
  '-an',
  outPath
])

await rm(framesDir, { recursive: true, force: true })

const stat = await import('node:fs').then(fs => fs.promises.stat(outPath))
console.log(`[svg-to-mp4] done: ${outPath} (${(stat.size / 1024 / 1024).toFixed(2)} MB)`)

function parseArgs(argv) {
  const out = {}
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]
    if (a.startsWith('--')) {
      const k = a.slice(2)
      const v = argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[++i] : true
      out[k] = v
    }
  }
  return out
}

function runFfmpeg(args) {
  return new Promise((resolve, reject) => {
    const p = spawn('ffmpeg', args, { stdio: ['ignore', 'inherit', 'inherit'] })
    p.on('exit', code => code === 0 ? resolve() : reject(new Error(`ffmpeg exited ${code}`)))
    p.on('error', reject)
  })
}
