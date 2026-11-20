//! Making local files durable. Lance on local disk doesn't fsync (risk K8, L11),
//! so ADB syncs the filesystem itself before it relies on what Lance wrote.

use std::path::Path;

/// Flush everything written on `path`'s filesystem to stable storage: `syncfs` on
/// Linux; elsewhere `sync`, which flushes every filesystem (a superset, so just as
/// durable).
pub fn sync_fs(path: &Path) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let f = std::fs::File::open(path)?;
        // SAFETY: `f` is an open descriptor for the duration of the call.
        if unsafe { libc::syncfs(f.as_raw_fd()) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        std::fs::metadata(path)?;
        // SAFETY: no arguments; `sync` can't fail.
        unsafe { libc::sync() };
    }
    Ok(())
}

/// [`sync_fs`] off the async runtime's threads.
pub async fn sync_fs_async(path: &Path) -> std::io::Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || sync_fs(&path)).await.map_err(std::io::Error::other)?
}

/// Raise this process's open-file limit to its hard limit (a database server holds a
/// socket per client connection plus its data files; container defaults are a soft
/// 1024, which 2,048 client connections ran out of on Hetzner, 15 Nov). Returns the
/// limit now in force.
pub fn raise_open_files() -> std::io::Result<u64> {
    let mut l = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    // SAFETY: `l` is a valid rlimit for the duration of each call.
    unsafe {
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut l) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        if l.rlim_cur < l.rlim_max {
            let want = libc::rlimit { rlim_cur: l.rlim_max, rlim_max: l.rlim_max };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &want) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            l.rlim_cur = l.rlim_max;
        }
    }
    Ok(l.rlim_cur as u64)
}

/// Lower the calling thread's CPU priority, for background work (compaction) that
/// must not take CPU from requests. Linux: the thread's nice value `nice` (0..19);
/// raising it needs no privilege. macOS: the thread's QoS class becomes "utility",
/// the scheduler's own way to mark background work (`nice` is not used there).
/// Best effort: an error leaves the priority as is.
pub fn lower_thread_priority(nice: i32) {
    #[cfg(target_os = "linux")]
    // SAFETY: `gettid` and `setpriority` on the calling thread, no pointers.
    unsafe {
        let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
        let _ = libc::setpriority(libc::PRIO_PROCESS, tid, nice);
    }
    #[cfg(target_vendor = "apple")]
    {
        let _ = nice;
        // SAFETY: sets the calling thread's own QoS class, no pointers.
        unsafe {
            let _ = libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_UTILITY, 0);
        }
    }
    #[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
    let _ = nice;
}
