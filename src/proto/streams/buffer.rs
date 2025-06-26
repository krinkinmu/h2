use std::sync::atomic::{AtomicUsize, Ordering};

use sharded_slab::Slab;

/// Buffers frames for multiple streams.
#[derive(Debug)]
pub struct Buffer<T> {
    slab: Slab<Slot<T>>,
    // The only reason we have this atomic is because sharded-slab crate does
    // not expose an interface that would allow to check if the Slab is empty
    // without a mutable reference.
    //
    // We don't actually rely on any atomicity or ordering guarantees here,
    // because the only place when this atomic is used is Buffer is_empty
    // method, which in turn is only called after we explicitly clear all the
    // queues of each stream under a global lock.
    //
    // Still in the future the proper solution would be to drop this atomic
    // all together.
    size: AtomicUsize,
}

/// A sequence of frames in a `Buffer`
#[derive(Debug)]
pub struct Deque {
    indices: Option<Indices>,
}

/// Tracks the head & tail for a sequence of frames in a `Buffer`.
#[derive(Debug, Default, Copy, Clone)]
struct Indices {
    head: usize,
    tail: usize,
}

#[derive(Debug)]
struct Slot<T> {
    value: T,
    // We use an atomic type here in order to make sure that Slot will be
    // Send and consequently all the types that include it can be Send
    // without explicitly creating a mutex around Buffer type.
    next: AtomicUsize,
}

impl<T> Buffer<T> {
    pub fn new() -> Self {
        Buffer { slab: Slab::new(), size: AtomicUsize::new(0) }
    }

    pub fn is_empty(&self) -> bool {
        self.size.load(Ordering::Relaxed) == 0
    }
}

impl Deque {
    pub fn new() -> Self {
        Deque { indices: None }
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_none()
    }

    pub fn push_back<T>(&mut self, buf: &Buffer<T>, value: T) {
        let key = buf.slab.insert(Slot { value, next: AtomicUsize::new(0) }).unwrap();
        let _ = buf.size.fetch_add(1, Ordering::Relaxed);

        match self.indices {
            Some(ref mut idxs) => {
                buf.slab.get(idxs.tail).unwrap().next.swap(key, Ordering::Relaxed);
                idxs.tail = key;
            }
            None => {
                self.indices = Some(Indices {
                    head: key,
                    tail: key,
                });
            }
        }
    }

    pub fn push_front<T>(&mut self, buf: &Buffer<T>, value: T) {
        let key = buf.slab.insert(Slot { value, next: AtomicUsize::new(0) }).unwrap();
        let _ = buf.size.fetch_add(1, Ordering::Relaxed);

        match self.indices {
            Some(ref mut idxs) => {
                buf.slab.get(key).unwrap().next.swap(idxs.head, Ordering::Relaxed);
                idxs.head = key;
            }
            None => {
                self.indices = Some(Indices {
                    head: key,
                    tail: key,
                });
            }
        }
    }

    pub fn pop_front<T>(&mut self, buf: &Buffer<T>) -> Option<T> {
        match self.indices {
            Some(mut idxs) => {
                let slot = buf.slab.take(idxs.head).unwrap();
                let _ = buf.size.fetch_sub(1, Ordering::Relaxed);

                if idxs.head == idxs.tail {
                    self.indices = None;
                } else {
                    idxs.head = slot.next.load(Ordering::Relaxed);
                    self.indices = Some(idxs);
                }

                Some(slot.value)
            }
            None => None,
        }
    }
}
