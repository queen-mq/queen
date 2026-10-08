// Path arithmetic for the Queen logo (assets/wordmark.py runs this).
// CoreGraphics does the unions, the offsets and the subtractions, and keeps the
// curves as curves, so every result is a clean outline and not a mask.
//
// stdin:  a JSON array of programs.   stdout: a JSON array, one result per program.
//
// A program   {"steps": [[op, name, ...]], "out": [name, ...]}
//   works on named shapes and gives the paths named in "out", in that order:
//     ["path",  name, "M x y ..."]    a shape from a path
//     ["union", name, a, b, ...]      all of a, b, ... (none: the empty shape)
//     ["minus", name, a, b]           a without b
//     ["both",  name, a, b]           where a and b overlap
//     ["grow",  name, a, d]           a with a margin of d all round
//     ["line",  name, a, w]           the outline of a as a band w wide, centred on it
//     ["keep",  name, a, min]         a without its pieces narrower than min
//
// A path is "M x y L x y C x1 y1 x2 y2 x y Z ...", absolute, in drawing units.
import CoreGraphics
import Foundation

func parse(_ text: String) -> CGPath {
    let path = CGMutablePath()
    let t = text.split(separator: " ").map(String.init)
    var i = 0
    func pt() -> CGPoint {
        let p = CGPoint(x: Double(t[i])!, y: Double(t[i + 1])!)
        i += 2
        return p
    }
    while i < t.count {
        let op = t[i]
        i += 1
        switch op {
        case "M": path.move(to: pt())
        case "L": path.addLine(to: pt())
        case "C":
            let a = pt(), b = pt(), end = pt()
            path.addCurve(to: end, control1: a, control2: b)
        case "Z": path.closeSubpath()
        default: fatalError("unknown op \(op)")
        }
    }
    return path
}

func num(_ v: CGFloat) -> String {
    var s = String(format: "%.1f", Double(v))
    if s.hasSuffix(".0") { s.removeLast(2) }
    return s == "-0" ? "0" : s
}

func write(_ path: CGPath) -> String {
    var out = ""
    path.applyWithBlock { e in
        let p = e.pointee.points
        switch e.pointee.type {
        case .moveToPoint: out += "M\(num(p[0].x)),\(num(p[0].y))"
        case .addLineToPoint: out += "L\(num(p[0].x)),\(num(p[0].y))"
        case .addQuadCurveToPoint: out += "Q\(num(p[0].x)),\(num(p[0].y)) \(num(p[1].x)),\(num(p[1].y))"
        case .addCurveToPoint:
            out += "C\(num(p[0].x)),\(num(p[0].y)) \(num(p[1].x)),\(num(p[1].y)) \(num(p[2].x)),\(num(p[2].y))"
        case .closeSubpath: out += "Z"
        @unknown default: break
        }
    }
    return out
}

/// The outline of the shape as a band `w` wide, centred on it.
func line(_ path: CGPath, width w: CGFloat) -> CGPath {
    path.copy(strokingWithWidth: w, lineCap: .round, lineJoin: .round, miterLimit: 4).normalized(using: .winding)
}

/// The shape with a margin of `d` all round it: its own outline, stroked, joined back to it.
func grown(_ path: CGPath, by d: CGFloat) -> CGPath {
    line(path, width: 2 * d).union(path)
}

/// The shape without its pieces narrower than `least`.
func kept(_ path: CGPath, least: CGFloat) -> CGPath {
    let out = CGMutablePath()
    for piece in path.componentsSeparated(using: .winding) {
        let box = piece.boundingBoxOfPath
        if min(box.width, box.height) >= least { out.addPath(piece) }
    }
    return out
}

func run(_ program: [String: Any]) -> [String] {
    var shapes: [String: CGPath] = [:]
    func shape(_ name: Any) -> CGPath {
        guard let s = shapes[name as! String] else { fatalError("no shape named \(name)") }
        return s
    }
    func number(_ v: Any) -> CGFloat { CGFloat((v as! NSNumber).doubleValue) }
    for step in program["steps"] as! [[Any]] {
        let op = step[0] as! String, name = step[1] as! String
        switch op {
        case "path": shapes[name] = parse(step[2] as! String).normalized(using: .winding)
        case "union":
            var all: CGPath = CGMutablePath()
            for other in step.dropFirst(2) { all = all.union(shape(other)) }
            shapes[name] = all
        case "minus": shapes[name] = shape(step[2]).subtracting(shape(step[3]))
        case "both": shapes[name] = shape(step[2]).intersection(shape(step[3]))
        case "grow": shapes[name] = grown(shape(step[2]), by: number(step[3]))
        case "line": shapes[name] = line(shape(step[2]), width: number(step[3]))
        case "keep": shapes[name] = kept(shape(step[2]), least: number(step[3]))
        default: fatalError("unknown step \(op)")
        }
    }
    return (program["out"] as! [String]).map { write(shape($0)) }
}

let input = FileHandle.standardInput.readDataToEndOfFile()
let programs = try! JSONSerialization.jsonObject(with: input) as! [[String: Any]]
FileHandle.standardOutput.write(try! JSONSerialization.data(withJSONObject: programs.map(run)))
