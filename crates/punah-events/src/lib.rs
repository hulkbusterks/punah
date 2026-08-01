//! Loss-aware, non-authoritative observations of committed storage facts.
//!
//! Publication is deliberately best-effort: a full subscriber queue or a
//! contended subscriber registry never delays the authoritative write path.
//! Consumers detect loss and rebuild from an authoritative snapshot.

#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex, Weak};

use punah_types::{ArtifactId, EntryState, Namespace, RecordLocation};

/// Why an artifact ceased to be live at its previous location.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemovalReason {
    Deleted,
    Expired,
}

/// An immutable fact that was authoritative before it was published.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LifecycleFact {
    ArtifactStored {
        key: Vec<u8>,
        namespace: Namespace,
        artifact_id: ArtifactId,
        location: RecordLocation,
        storage_sequence: u64,
    },
    ArtifactRemoved {
        key: Vec<u8>,
        namespace: Namespace,
        artifact_id: ArtifactId,
        storage_sequence: u64,
        reason: RemovalReason,
    },
    ArtifactRelocated {
        key: Vec<u8>,
        namespace: Namespace,
        artifact_id: ArtifactId,
        old_location: RecordLocation,
        new_location: RecordLocation,
        storage_sequence: u64,
    },
}

/// One observation in a publisher-local ordered stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleEvent {
    pub cursor: u64,
    pub fact: LifecycleFact,
}

/// One authoritative entry used to rebuild an observer after lag.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactLocationSnapshot {
    pub key: Vec<u8>,
    pub namespace: Namespace,
    pub artifact_id: ArtifactId,
    pub location: RecordLocation,
    pub storage_sequence: u64,
    pub state: EntryState,
}

/// A point-in-time authority view and the event cursor it supersedes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleSnapshot {
    pub through_cursor: u64,
    pub artifacts: Vec<ArtifactLocationSnapshot>,
}

struct SubscriberSink {
    sender: SyncSender<LifecycleEvent>,
    lagged: Arc<AtomicBool>,
}

struct Shared {
    cursor: AtomicU64,
    subscribers: Mutex<Vec<Weak<SubscriberSink>>>,
}

/// Cloneable handle used only after an operation becomes authoritative.
#[derive(Clone)]
pub struct EventPublisher {
    shared: Arc<Shared>,
}

/// Registration point for observers and owner of the publisher-local cursor.
pub struct EventBus {
    shared: Arc<Shared>,
    subscriber_capacity: usize,
}

impl EventBus {
    /// Creates a bus whose subscribers each have `subscriber_capacity` slots.
    #[must_use]
    pub fn new(subscriber_capacity: usize) -> Self {
        Self {
            shared: Arc::new(Shared {
                cursor: AtomicU64::new(0),
                subscribers: Mutex::new(Vec::new()),
            }),
            subscriber_capacity,
        }
    }

    #[must_use]
    pub fn publisher(&self) -> EventPublisher {
        EventPublisher {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Adds an observer. Registration may wait; authoritative publication does not.
    #[must_use]
    pub fn subscribe(&self) -> Subscription {
        let (sender, receiver) = mpsc::sync_channel(self.subscriber_capacity);
        let lagged = Arc::new(AtomicBool::new(false));
        let sink = Arc::new(SubscriberSink {
            sender,
            lagged: Arc::clone(&lagged),
        });
        self.shared
            .subscribers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Arc::downgrade(&sink));
        Subscription {
            receiver,
            _sink: sink,
            lagged,
            shared: Arc::clone(&self.shared),
            observed_cursor: self.current_cursor(),
            pending: None,
        }
    }

    #[must_use]
    pub fn current_cursor(&self) -> u64 {
        self.shared.cursor.load(Ordering::Acquire)
    }
}

impl EventPublisher {
    /// Best-effort publication. It never waits for a registry or consumer.
    pub fn publish(&self, fact: LifecycleFact) {
        let cursor = self.shared.cursor.fetch_add(1, Ordering::AcqRel) + 1;
        let event = LifecycleEvent { cursor, fact };
        let Ok(mut subscribers) = self.shared.subscribers.try_lock() else {
            return;
        };
        subscribers.retain(|weak| {
            let Some(sink) = weak.upgrade() else {
                return false;
            };
            match sink.sender.try_send(event.clone()) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => sink.lagged.store(true, Ordering::Release),
                Err(TrySendError::Disconnected(_)) => return false,
            }
            true
        });
    }

    #[must_use]
    pub fn current_cursor(&self) -> u64 {
        self.shared.cursor.load(Ordering::Acquire)
    }
}

/// Non-authoritative observer of lifecycle facts.
pub struct Subscription {
    receiver: Receiver<LifecycleEvent>,
    // Keeps the weak registry entry and its sender alive with this receiver.
    _sink: Arc<SubscriberSink>,
    lagged: Arc<AtomicBool>,
    shared: Arc<Shared>,
    observed_cursor: u64,
    pending: Option<LifecycleEvent>,
}

/// Outcome of a non-blocking observation attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Poll {
    Event(LifecycleEvent),
    Empty,
    /// At least one fact may be missing; discard derived state and resnapshot.
    Lagged {
        observed: u64,
        published: u64,
    },
    Disconnected,
}

impl Subscription {
    #[must_use]
    pub fn try_next(&mut self) -> Poll {
        if self.lagged.swap(false, Ordering::AcqRel) {
            return self.lag();
        }
        let received = self.pending.take().map_or_else(
            || self.receiver.try_recv(),
            Result::<LifecycleEvent, TryRecvError>::Ok,
        );
        match received {
            Ok(event) if event.cursor == self.observed_cursor.saturating_add(1) => {
                self.observed_cursor = event.cursor;
                Poll::Event(event)
            }
            Ok(_) => self.lag(),
            Err(TryRecvError::Empty) => {
                let published = self.shared.cursor.load(Ordering::Acquire);
                if published > self.observed_cursor {
                    self.lag()
                } else {
                    Poll::Empty
                }
            }
            Err(TryRecvError::Disconnected) => Poll::Disconnected,
        }
    }

    /// Marks a completed authoritative resnapshot as the new stream baseline.
    pub fn resynced_through(&mut self, cursor: u64) {
        self.observed_cursor = cursor;
        self.lagged.store(false, Ordering::Release);
        self.pending = None;
        while let Ok(event) = self.receiver.try_recv() {
            if event.cursor > cursor {
                self.pending = Some(event);
                break;
            }
        }
    }

    #[must_use]
    pub const fn observed_cursor(&self) -> u64 {
        self.observed_cursor
    }

    fn lag(&self) -> Poll {
        Poll::Lagged {
            observed: self.observed_cursor,
            published: self.shared.cursor.load(Ordering::Acquire),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use punah_types::SegmentId;

    fn location(offset: u64) -> RecordLocation {
        RecordLocation {
            segment_id: SegmentId(1),
            generation: 1,
            offset,
            stored_len: 10,
            logical_len: 2,
        }
    }

    fn fact(offset: u64) -> LifecycleFact {
        let byte = u8::try_from(offset).expect("test offset fits in one byte");
        LifecycleFact::ArtifactStored {
            key: vec![byte],
            namespace: Namespace::new("test").expect("namespace"),
            artifact_id: ArtifactId::new([byte; 32]),
            location: location(offset),
            storage_sequence: offset,
        }
    }

    #[test]
    fn observer_receives_ordered_facts() {
        let bus = EventBus::new(2);
        let mut observer = bus.subscribe();
        bus.publisher().publish(fact(1));
        assert!(matches!(observer.try_next(), Poll::Event(event) if event.cursor == 1));
        assert_eq!(observer.try_next(), Poll::Empty);
    }

    #[test]
    fn bounded_queue_reports_lag_instead_of_blocking_publisher() {
        let bus = EventBus::new(1);
        let mut observer = bus.subscribe();
        let publisher = bus.publisher();
        publisher.publish(fact(1));
        publisher.publish(fact(2));
        assert_eq!(
            observer.try_next(),
            Poll::Lagged {
                observed: 0,
                published: 2
            }
        );
        observer.resynced_through(2);
        assert_eq!(observer.try_next(), Poll::Empty);
    }

    #[test]
    fn late_subscriber_starts_at_current_cursor() {
        let bus = EventBus::new(1);
        bus.publisher().publish(fact(1));
        let mut observer = bus.subscribe();
        assert_eq!(observer.observed_cursor(), 1);
        assert_eq!(observer.try_next(), Poll::Empty);
    }

    #[test]
    fn resync_keeps_events_newer_than_snapshot_watermark() {
        let bus = EventBus::new(3);
        let mut observer = bus.subscribe();
        let publisher = bus.publisher();
        publisher.publish(fact(1));
        let snapshot_cursor = publisher.current_cursor();
        publisher.publish(fact(2));

        observer.resynced_through(snapshot_cursor);
        assert!(matches!(
            observer.try_next(),
            Poll::Event(event) if event.cursor == 2
        ));
    }
}
