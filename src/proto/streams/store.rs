use super::*;

use indexmap::{self, IndexMap};

use std::collections::VecDeque;
use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug)]
struct Inner {
    slab: slab::Slab<Arc<Mutex<Stream>>>,
    ids: IndexMap<StreamId, SlabIndex>,
}

/// Storage for streams
#[derive(Debug)]
pub(super) struct Store {
    inner: Mutex<Inner>,
}

/// "Pointer" to an entry in the store
pub(super) struct Ptr<'a> {
    key: Key,
    store: &'a Store,
    stream: Arc<Mutex<Stream>>,
}

pub(super) struct PtrMut<'a> {
    key: Key,
    store: &'a Store,
    stream: MutexGuard<'a, Stream>,
}

/// References an entry in the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Key {
    index: SlabIndex,
    stream_id: StreamId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SlabIndex(u32);

#[derive(Debug)]
pub(super) struct Queue<N> {
    queue: VecDeque<StreamId>,
    _p: PhantomData<N>,
}

pub(super) trait Next {
    fn is_queued(stream: &Stream) -> bool;

    fn set_queued(stream: &mut Stream, val: bool);
}

pub(super) enum Entry<'a> {
    Occupied(OccupiedEntry),
    Vacant(VacantEntry<'a>),
}

pub(super) struct OccupiedEntry {
    key: Key,
}

pub(super) struct VacantEntry<'a> {
    guard: MutexGuard<'a, Inner>,
    stream_id: StreamId,
}

pub(super) trait Resolve {
    fn resolve(&self, key: Key) -> Ptr;

    fn store(&self) -> &Store;
}

// ===== impl Store =====

impl Store {
    pub fn new() -> Self {
        let inner = Inner {
            slab: slab::Slab::new(),
            ids: IndexMap::new(),
        };
        Store { inner: Mutex::new(inner) }
    }

    pub fn find(&self, id: &StreamId) -> Option<Ptr<'_>> {
        let inner = self.inner.lock().unwrap();
        let index = match inner.ids.get(id) {
            Some(key) => *key,
            None => return None,
        };
        let stream = inner.slab.get(index.0 as usize)
            .unwrap_or_else(|| {
                panic!("dangling store key for stream_id={:?}", id);
            });

        Some(Ptr {
            key: Key {
                index,
                stream_id: *id,
            },
            stream: stream.clone(),
            store: self,
        })
    }

    pub fn insert(&self, id: StreamId, val: Stream) -> Ptr<'_> {
        let stream = Arc::new(Mutex::new(val));
        let index = {
            let mut inner = self.inner.lock().unwrap();
            let index = SlabIndex(inner.slab.insert(stream.clone()) as u32);
            assert!(inner.ids.insert(id, index).is_none());
            index
        };
        Ptr {
            key: Key {
                index,
                stream_id: id,
            },
            stream: stream,
            store: self,
        }
    }

    pub fn find_entry(&self, id: StreamId) -> Entry {
        let inner = self.inner.lock().unwrap();
        if let Some(index) = inner.ids.get(&id).map(|index| *index) {
            Entry::Occupied(OccupiedEntry {
                key: Key { stream_id: id, index: index },
            })
        } else {
            Entry::Vacant(VacantEntry {
                guard: inner,
                stream_id: id,
            })
        }
    }

    #[allow(clippy::blocks_in_conditions)]
    pub(crate) fn for_each<F>(&mut self, mut f: F)
    where
        F: FnMut(Ptr),
    {
        match self.try_for_each(|ptr| {
            f(ptr);
            Ok::<_, Infallible>(())
        }) {
            Ok(()) => (),
            #[allow(unused)]
            Err(infallible) => match infallible {},
        }
    }

    fn all_streams(&self) -> Vec<Ptr<'_>> {
        let inner = self.inner.lock().unwrap();
        let mut ptrs = Vec::with_capacity(inner.ids.len());

        for (stream_id, index) in inner.ids.iter() {
            ptrs.push(Ptr {
                key: Key { index: *index, stream_id: *stream_id },
                stream: inner.slab.get(index.0 as usize).unwrap().clone(),
                store: self,
            });
        }

        ptrs
    }

    pub fn try_for_each<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(Ptr) -> Result<(), E>,
    {
        let ptrs = self.all_streams();

        // all_streams takes a snapshort of streams available in the store
        // and we then can iterate over them without holding a lock.
        //
        // Thus if for whatever reason the function f need to call into
        // Store again it would not cause a deadlock.
        for ptr in ptrs.into_iter() {
            f(ptr)?;
        }

        Ok(())
    }

    pub fn index(&self, key: Key) -> Arc<Mutex<Stream>> {
        let inner = self.inner.lock().unwrap();
        inner.slab.get(key.index.0 as usize)
            .unwrap_or_else(|| {
                panic!("dangling store key for stream_id={:?}", key.stream_id);
            }).clone()
    }

    pub fn remove(&self, key: Key) -> StreamId {
        let mut inner = self.inner.lock().unwrap();

        // The stream must have been unlinked before this point
        debug_assert!(!inner.ids.contains_key(&key.stream_id));

        _ = inner.slab.remove(key.index.0 as usize);
        key.stream_id
    }

    pub fn unlink(&self, key: Key) {
        let mut inner = self.inner.lock().unwrap();

        inner.ids.swap_remove(&key.stream_id);
    }
}

impl Resolve for Store {
    fn resolve(&self, key: Key) -> Ptr<'_> {
        let inner = self.inner.lock().unwrap();
        let stream = inner.slab.get(key.index.0 as usize)
            .unwrap_or_else(|| {
                panic!("dangling store key for stream_id={:?}", key.stream_id);
            });
        Ptr { key, store: self, stream: stream.clone() }
    }

    fn store(&self) -> &Store {
        self
    }
}

impl Store {
    #[cfg(feature = "unstable")]
    pub fn num_active_streams(&self) -> usize {
        self.ids.len()
    }
}

// While running h2 unit/integration tests, enable this debug assertion.
//
// In practice, we don't need to ensure this. But the integration tests
// help to make sure we've cleaned up in cases where we could (like, the
// runtime isn't suddenly dropping the task for unknown reasons).
#[cfg(feature = "unstable")]
impl Drop for Store {
    fn drop(&mut self) {
        use std::thread;

        if !thread::panicking() {
            let inner = self.inner.lock().unwrap();
            debug_assert!(inner.slab.is_empty());
        }
    }
}

// ===== impl Queue =====

impl<N> Queue<N>
where
    N: Next,
{
    pub fn new() -> Self {
        Queue {
            queue: VecDeque::new(),
            _p: PhantomData,
        }
    }

    pub fn take(&mut self) -> Self {
        Queue {
            queue: mem::take(&mut self.queue),
            _p: PhantomData,
        }
    }

    /// Queue the stream.
    ///
    /// If the stream is already contained by the list, return `false`.
    pub fn push(&mut self, stream: &mut store::PtrMut) -> bool {
        tracing::trace!("Queue::push_back");

        if N::is_queued(stream) {
            tracing::trace!(" -> already queued");
            return false;
        }

        N::set_queued(stream, true);
        self.queue.push_back(stream.id);
        true
    }

    /// Queue the stream
    ///
    /// If the stream is already contained by the list, return `false`.
    pub fn push_front(&mut self, stream: &mut store::PtrMut) -> bool {
        tracing::trace!("Queue::push_front");

        if N::is_queued(stream) {
            tracing::trace!(" -> already queued");
            return false;
        }

        N::set_queued(stream, true);
        self.queue.push_front(stream.id);
        true
    }

    pub fn pop<'a, R>(&mut self, resolve: &'a R) -> Option<store::Ptr<'a>>
    where
        R: Resolve,
    {
        if let Some(stream_id) = self.queue.pop_front() {
            let store = resolve.store();
            let ptr = store.find(&stream_id).unwrap();
            {
                let mut stream = ptr.lock();
                debug_assert!(N::is_queued(&stream));
                N::set_queued(&mut stream, false);
            }
            return Some(ptr);
        }

        None
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn pop_if<'a, R, F>(&mut self, resolve: &'a R, f: F) -> Option<store::Ptr<'a>>
    where
        R: Resolve,
        F: Fn(&Stream) -> bool,
    {
        if let Some(stream_id) = self.queue.front() {
            let store = resolve.store();
            let ptr = store.find(stream_id).unwrap();
            let mut stream = ptr.lock();
            let should_pop = f(&stream);
            if should_pop {
                let _ = self.queue.pop_front();
                debug_assert!(N::is_queued(&stream));
                N::set_queued(&mut stream, false);
                drop(stream);
                return Some(ptr);
            }
        }

        None
    }
}

// ===== impl Ptr =====

impl<'a> Ptr<'a> {
    /// Returns the Key associated with the stream
    pub fn key(&self) -> Key {
        self.key
    }

    pub fn lock(&self) -> PtrMut<'_> {
        PtrMut {
            key: self.key,
            store: self.store,
            stream: self.stream.lock().unwrap(),
        }
    }

    pub fn stream(&self) -> Arc<Mutex<Stream>> {
        self.stream.clone()
    }
}

impl<'a> PtrMut<'a> {
    pub fn key(&self) -> Key {
        self.key
    }

    pub fn store_mut(&self) -> &Store {
        self.store
    }

    pub fn remove(self) -> StreamId {
        self.store.remove(self.key)
    }

    pub fn unlink(&self) {
        self.store.unlink(self.key);
    }
}

impl<'a> Deref for PtrMut<'a> {
    type Target = Stream;

    fn deref(&self) -> &'_ Self::Target {
        &self.stream
    }
}

impl<'a> DerefMut for PtrMut<'a> {
    fn deref_mut(&mut self) -> &'_ mut Self::Target {
        &mut self.stream
    }
}

impl<'a> fmt::Debug for Ptr<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        (*self.stream().lock().unwrap()).fmt(fmt)
    }
}

impl<'a> fmt::Debug for PtrMut<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        (*self.stream).fmt(fmt)
    }
}

// ===== impl OccupiedEntry =====

impl OccupiedEntry {
    pub fn key(&self) -> Key {
        self.key
    }
}

// ===== impl VacantEntry =====

impl<'a> VacantEntry<'a> {
    pub fn insert(mut self, value: Stream) -> Key {
        assert_eq!(self.stream_id, value.id);

        // Insert the value in the slab
        let stream = Arc::new(Mutex::new(value));
        let index = SlabIndex(self.guard.slab.insert(stream) as u32);
        assert!(self.guard.ids.insert(self.stream_id, index).is_none());

        Key { index, stream_id: self.stream_id }
    }
}
