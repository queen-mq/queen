//! A bus error while a store call walks the LMDB file: one FATAL line naming
//! the file, then exit 1 — a refusal like every other, not a silent
//! "Bus error".
//!
//! LMDB reads `data.mdb` through a memory map. A page the committed B-tree
//! uses but the file no longer holds (the file was truncated), or that the
//! disk cannot read, raises SIGBUS on the reading thread the moment LMDB
//! touches it, before a value checksum ([`super::integrity`]) can look at
//! anything. Jepsen's corrupt nemesis saw a node die that way, with no log
//! line, on 39 of 134 refusals (P10, 2026-09-26: `rc=135` on a truncated
//! `data.mdb`).
//!
//! The file's length cannot be checked up front instead. LMDB never writes a
//! page it allocated and freed in the same transaction (`P_LOOSE`, skipped by
//! `mdb_page_flush`), yet the meta's last page counts it: a healthy file can
//! end before the last page its meta names, and a length check would refuse
//! good stores.
//!
//! So each store call that walks the file holds a [`MapWatch`] on its thread
//! for the length of the walk. A SIGBUS on a watched thread writes the watch's
//! [`Notice`] to stderr and exits 1 (`_exit`: nothing more is safe inside a
//! signal handler). A SIGBUS on any other thread goes to the action installed
//! before this one — Rust's stack-overflow check, else the default — as if
//! this handler were not there.

use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

/// The line a watch prints, rendered once per store file.
#[derive(Clone)]
pub(crate) struct Notice(Arc<[u8]>);

impl Notice {
    /// For the store directory `dir`, whose file is `dir/data.mdb`.
    pub(crate) fn for_dir(dir: &Path) -> Notice {
        let line = format!(
            "FATAL store: bus error (SIGBUS) reading {}: the file does not hold pages its \
             committed B-tree uses (truncated, or the disk cannot read them). This node \
             refuses to run on it: restore the file from a copy, or wipe this node's data \
             directory and let it rejoin from a peer.\n",
            dir.join("data.mdb").display()
        );
        Notice(Arc::from(line.into_bytes()))
    }
}

/// While alive, a SIGBUS on the thread that created it prints its notice and
/// exits 1. Not `Send`: the watch names its thread, so it must end there too.
#[must_use]
pub(crate) struct MapWatch {
    #[cfg(unix)]
    slot: Option<usize>,
    /// Keeps the notice's bytes alive while a handler may print them.
    _notice: Notice,
    _this_thread: PhantomData<*const ()>,
}

impl MapWatch {
    pub(crate) fn new(notice: &Notice) -> MapWatch {
        MapWatch {
            #[cfg(unix)]
            slot: imp::claim(&notice.0),
            _notice: notice.clone(),
            _this_thread: PhantomData,
        }
    }
}

impl Drop for MapWatch {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(i) = self.slot {
            imp::release(i);
        }
    }
}

#[cfg(unix)]
mod imp {
    use std::ptr;
    use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
    use std::sync::{Once, OnceLock};

    /// Watches alive at once: per store, its open, scrub, checkpoint writer
    /// and a snapshot copy, with room for many groups. A watch that finds no
    /// free slot protects nothing (the plain bus error, as before).
    const SLOTS: usize = 64;

    /// Claimed but not yet published: no thread has this id.
    const CLAIMING: usize = usize::MAX;

    struct Slot {
        /// The watching thread's `pthread_self`; 0 = free.
        thread: AtomicUsize,
        msg: AtomicPtr<u8>,
        len: AtomicUsize,
    }

    #[allow(clippy::declare_interior_mutable_const)]
    const FREE: Slot = Slot {
        thread: AtomicUsize::new(0),
        msg: AtomicPtr::new(ptr::null_mut()),
        len: AtomicUsize::new(0),
    };

    static SLOT: [Slot; SLOTS] = [FREE; SLOTS];
    static INSTALL: Once = Once::new();
    static PREVIOUS: OnceLock<libc::sigaction> = OnceLock::new();

    fn this_thread() -> usize {
        // SAFETY: no preconditions; it reads the calling thread's id.
        unsafe { libc::pthread_self() as usize }
    }

    pub(super) fn claim(msg: &[u8]) -> Option<usize> {
        INSTALL.call_once(install);
        for (i, s) in SLOT.iter().enumerate() {
            if s.thread
                .compare_exchange(0, CLAIMING, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                s.msg.store(msg.as_ptr().cast_mut(), Ordering::Relaxed);
                s.len.store(msg.len(), Ordering::Relaxed);
                s.thread.store(this_thread(), Ordering::Release);
                return Some(i);
            }
        }
        None
    }

    pub(super) fn release(i: usize) {
        let s = &SLOT[i];
        s.msg.store(ptr::null_mut(), Ordering::Relaxed);
        s.len.store(0, Ordering::Relaxed);
        s.thread.store(0, Ordering::Release);
    }

    fn install() {
        // SAFETY: `sigaction` on zeroed plain-data structs. The previous action
        // is kept first, so the handler always has it to hand faults back to.
        unsafe {
            let mut old: libc::sigaction = std::mem::zeroed();
            if libc::sigaction(libc::SIGBUS, ptr::null(), &mut old) != 0 {
                return;
            }
            let _ = PREVIOUS.set(old);
            let mut sa: libc::sigaction = std::mem::zeroed();
            sa.sa_sigaction = on_sigbus as *const () as libc::sighandler_t;
            sa.sa_flags = libc::SA_SIGINFO | libc::SA_ONSTACK;
            libc::sigemptyset(&mut sa.sa_mask);
            libc::sigaction(libc::SIGBUS, &sa, ptr::null_mut());
        }
    }

    /// Only async-signal-safe calls in here: atomics, `write`, `_exit`,
    /// `sigaction`.
    extern "C" fn on_sigbus(
        sig: libc::c_int,
        _info: *mut libc::siginfo_t,
        _ctx: *mut libc::c_void,
    ) {
        let me = this_thread();
        for s in SLOT.iter() {
            if s.thread.load(Ordering::Acquire) == me {
                let msg = s.msg.load(Ordering::Relaxed);
                let len = s.len.load(Ordering::Relaxed);
                // SAFETY: the watch that published `msg` is alive on this very
                // thread (the one that faulted) and holds the bytes.
                let line = unsafe { std::slice::from_raw_parts(msg.cast_const(), len) };
                write_all(line);
                // SAFETY: ends the process; nothing to uphold.
                unsafe { libc::_exit(1) }
            }
        }
        // Not a watched walk. Hand the signal back to the action that was
        // there before: the faulting access repeats when this returns, and
        // that action gets it, as if this handler had never been installed.
        // SAFETY: `sigaction` with a struct `install` read from the kernel.
        unsafe {
            match PREVIOUS.get() {
                Some(prev) => {
                    libc::sigaction(sig, prev, ptr::null_mut());
                }
                None => {
                    libc::signal(sig, libc::SIG_DFL);
                }
            }
        }
    }

    fn write_all(mut line: &[u8]) {
        while !line.is_empty() {
            // SAFETY: `line` is a live slice.
            let n = unsafe { libc::write(2, line.as_ptr().cast(), line.len()) };
            if n < 0 {
                // Reading errno allocates nothing.
                if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return;
            }
            line = &line[n as usize..];
        }
    }
}
