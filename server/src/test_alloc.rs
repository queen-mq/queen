//! Test builds only: the heap one closure takes on the calling thread.
//!
//! The library's test binary runs on the system allocator (`main.rs` sets
//! mimalloc for the broker itself). This wraps it and counts only on a thread
//! inside [`peak_during`], so every other test allocates as it always did.
//! The binary's test target compiles the same tests over mimalloc, where
//! nothing is counted and [`peak_during`] answers `None`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) struct Counting;

/// Set by the first allocation through [`Counting`]: it is the process
/// allocator.
static INSTALLED: AtomicBool = AtomicBool::new(false);

thread_local! {
    static ON: Cell<bool> = const { Cell::new(false) };
    static NOW: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

fn note(delta: isize) {
    if ON.try_with(Cell::get).unwrap_or(false) {
        let now = NOW.try_with(|n| {
            let v = n.get() + delta;
            n.set(v);
            v
        });
        if let Ok(now) = now {
            let _ = PEAK.try_with(|p| p.set(p.get().max(now)));
        }
    }
}

// SAFETY: every call is passed to `System` unchanged; the counting touches
// only thread-locals that need no allocation and no destructor.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if !INSTALLED.load(Ordering::Relaxed) {
            INSTALLED.store(true, Ordering::Relaxed);
        }
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            note(layout.size() as isize);
        }
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            note(layout.size() as isize);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        note(-(layout.size() as isize));
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            note(new_size as isize - layout.size() as isize);
        }
        p
    }
}

/// What `f` returns, and the most heap this thread held at once while it ran,
/// in bytes above what it held when it began; `None` when [`Counting`] is not
/// the process allocator. A `realloc` is counted as its net growth, so a peak
/// can be low by one buffer's old size while it moves.
pub(crate) fn peak_during<R>(f: impl FnOnce() -> R) -> (R, Option<usize>) {
    NOW.with(|n| n.set(0));
    PEAK.with(|p| p.set(0));
    ON.with(|o| o.set(true));
    let r = f();
    ON.with(|o| o.set(false));
    let peak = PEAK.with(Cell::get).max(0) as usize;
    (r, INSTALLED.load(Ordering::Relaxed).then_some(peak))
}
