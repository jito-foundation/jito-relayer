use std::{
    alloc::{alloc_zeroed, Layout},
    net::SocketAddr,
    ops::DerefMut,
};

use libc::{iovec, mmsghdr, sockaddr_storage};

pub const EVENT_CNT: usize = 4096;
pub const PKT_BUF_SZ: usize = 4096;

const _: () = assert!(EVENT_CNT <= u16::MAX as usize);

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

pub(crate) struct PktBuf {
    pub bufs: [[u8; PKT_BUF_SZ]; EVENT_CNT],
    pub iovs: [iovec; EVENT_CNT],
    pub msgs: [mmsghdr; EVENT_CNT],
    pub addrs: [sockaddr_storage; EVENT_CNT],
    _phantom: std::marker::PhantomPinned,
}

impl PktBuf {
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

    pub fn get_packet(&mut self, i: usize) -> Option<(&mut [u8], SocketAddr)> {
        let data = &mut self.bufs[i][..self.msgs[i].msg_len as usize];
        let sender = unsafe {
            socket2::SockAddr::new(self.addrs[i], self.msgs[i].msg_hdr.msg_namelen).as_socket()?
        };
        Some((data, sender))
    }

    pub fn reset_iovlens(&mut self, cnt: usize) {
        for i in 0..cnt {
            self.iovs[i].iov_len = PKT_BUF_SZ;
        }
    }
}
