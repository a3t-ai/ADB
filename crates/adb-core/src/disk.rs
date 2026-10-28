//! Making local files durable. Lance on local disk doesn't fsync (risk K8, L11),
//! so ADB syncs the filesystem itself before it relies on what Lance wrote.

use std::path::Path;

/// Flush everything written on `path`'s filesystem to stable storage (`syncfs`).
pub fn sync_fs(path: &Path) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    let f = std::fs::File::open(path)?;
    // SAFETY: `f` is an open descriptor for the duration of the call.
    if unsafe { libc::syncfs(f.as_raw_fd()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// [`sync_fs`] off the async runtime's threads.
pub async fn sync_fs_async(path: &Path) -> std::io::Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || sync_fs(&path)).await.map_err(std::io::Error::other)?
}
