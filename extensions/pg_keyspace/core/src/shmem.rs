//! POSIX shared-memory segment. Stands in for the plan's `GetNamedDSMSegment`
//!. The segment is created MAP_SHARED and named, so:
//!
//!   * each slot worker maps the same segment and operates on its own disjoint
//!     partition ("written by exactly one worker, no locks, no atomics");
//!   * a *separate* process — modelling a Postgres backend calling
//!     `supacache.get` — can attach the same segment and read an entry
//!     directly, with no socket and no copy. `latency_probe --inproc` measures
//!     exactly that path.

use std::ffi::CString;
use std::io;

pub struct Shmem {
    ptr: *mut u8,
    len: usize,
    name: CString,
    owner: bool,
}

// The segment is a fixed arena; the store layer enforces the single-writer
// discipline that makes concurrent access sound.
unsafe impl Send for Shmem {}
unsafe impl Sync for Shmem {}

impl Shmem {
    /// Create (and zero) a fresh segment, unlinking any stale one first.
    pub fn create(name: &str, len: usize) -> io::Result<Shmem> {
        let cname = CString::new(format!("/{name}")).unwrap();
        unsafe {
            libc::shm_unlink(cname.as_ptr()); // ignore ENOENT
            let fd = libc::shm_open(
                cname.as_ptr(),
                libc::O_CREAT | libc::O_RDWR | libc::O_EXCL,
                0o600,
            );
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::ftruncate(fd, len as libc::off_t) != 0 {
                let e = io::Error::last_os_error();
                libc::close(fd);
                return Err(e);
            }
            let ptr = libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            );
            libc::close(fd);
            if ptr == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }
            std::ptr::write_bytes(ptr as *mut u8, 0, len);
            Ok(Shmem {
                ptr: ptr as *mut u8,
                len,
                name: cname,
                owner: true,
            })
        }
    }

    /// Attach an existing segment read/write (used by extra workers).
    pub fn attach(name: &str, len: usize) -> io::Result<Shmem> {
        let cname = CString::new(format!("/{name}")).unwrap();
        unsafe {
            let fd = libc::shm_open(cname.as_ptr(), libc::O_RDWR, 0o600);
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let ptr = libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            );
            libc::close(fd);
            if ptr == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }
            Ok(Shmem {
                ptr: ptr as *mut u8,
                len,
                name: cname,
                owner: false,
            })
        }
    }

    #[inline]
    pub fn base(&self) -> *mut u8 {
        self.ptr
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
}

impl Drop for Shmem {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.len);
            if self.owner {
                libc::shm_unlink(self.name.as_ptr());
            }
        }
    }
}
