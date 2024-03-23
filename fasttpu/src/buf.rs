use std::{
    alloc::{alloc_zeroed, Layout},
    net::SocketAddr,
    ops::DerefMut,
};

use libc::{iovec, mmsghdr, sockaddr_storage};

/// The depth of the packet queue, replenished every poll.
pub const EVENT_CNT: usize = 4096;

/// The UDP datagram MTU.
pub const PKT_BUF_SZ: usize = 4096;

/// The max serialized size of a transaction.
pub const TXN_MAX_SZ: usize = 1228; // TODO is this still accurate?

/// Number of concurrent in-flight streams.
pub const REASM_DEPTH: usize = 1024;

const _: () = assert!(EVENT_CNT <= u16::MAX as usize);

/// Safely manages a `PktBuf`.  Do not clone.
pub(crate) struct PktBufBox {
    inner: *mut PktBuf,
}

impl PktBufBox {
    pub(crate) fn new() -> Self {
        unsafe {
            let inner = alloc_zeroed(Layout::new::<PktBuf>()) as *mut PktBuf;
            assert!(!inner.is_null(), "alloc failure");
            let mut this = Self { inner };
            this.deref_mut().init();
            this
        }
    }
}

unsafe impl Send for PktBufBox {}

impl Drop for PktBufBox {
    fn drop(&mut self) {
        unsafe {
            std::ptr::drop_in_place(self.inner);
            std::alloc::dealloc(self.inner as *mut u8, Layout::new::<PktBuf>());
        }
    }
}

impl std::ops::Deref for PktBufBox {
    type Target = PktBuf;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner }
    }
}

impl std::ops::DerefMut for PktBufBox {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.inner }
    }
}

/// Huge struct containing reusable buffers for recvmmsg(2) and
/// sendmmsg(2).  Do not declare this variable using default Rust
/// syntax.  Due to fundamental language flaws in Rust (lack of
/// placement new) simply declaring this variable will crash the
/// program.  Instead, one shouild manually allocate a memory region
/// with compatible layout on the heap and then "unsafely" reinterpret
/// it as a `*mut PktBuf`. `PktBufBox` does this for you.
pub(crate) struct PktBuf {
    pub bufs: [[u8; PKT_BUF_SZ]; EVENT_CNT],
    pub iovs: [iovec; EVENT_CNT],
    pub msgs: [mmsghdr; EVENT_CNT],
    pub addrs: [sockaddr_storage; EVENT_CNT],
    _phantom: std::marker::PhantomPinned,
}

impl PktBuf {
    /// init forms
    fn init(&mut self) {
        for i in 0..EVENT_CNT {
            self.iovs[i].iov_base = self.bufs[i].as_mut_ptr() as *mut _;
            self.iovs[i].iov_len = PKT_BUF_SZ;
            self.msgs[i].msg_hdr.msg_name = &mut self.addrs[i] as *mut _ as *mut _;
            self.msgs[i].msg_hdr.msg_namelen = std::mem::size_of::<sockaddr_storage>() as _;
            self.msgs[i].msg_hdr.msg_iov = &mut self.iovs[i];
            self.msgs[i].msg_hdr.msg_iovlen = 1;
        }
    }

    /// After a successful recvmmsg for that packet index, get_packet
    /// provides a mutable slice to that packet buffer.  The number of
    /// bytes in that slice is the UDP datagram size (truncated to MTU
    /// for oversize packets).  Also returns the sender address.
    pub fn get_packet(&mut self, i: usize) -> Option<(&mut [u8], SocketAddr)> {
        let data = &mut self.bufs[i][..self.msgs[i].msg_len as usize];
        let sender = unsafe {
            socket2::SockAddr::new(self.addrs[i], self.msgs[i].msg_hdr.msg_namelen).as_socket()?
        };
        Some((data, sender))
    }

    /// The kernel might change the iov_len field to inform us how much
    /// buffer space it consumed.  Before recycling the packet buffers,
    /// we need to reset the length fields to their original size
    /// For better performance, we only reset the packet indices that
    /// were known to be used (in 0..cnt).
    pub fn reset_iovlens(&mut self, cnt: usize) {
        for i in 0..cnt {
            self.iovs[i].iov_len = PKT_BUF_SZ;
        }
    }
}
