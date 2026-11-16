//! Who is connected to this gateway instance, and fan-out to them.
//!
//! Each WebSocket connection owns a small bounded queue. Fan-out never waits on a client:
//! if a connection's queue is full, the client is too slow, so it's dropped from the hub.
//! Its session then closes the socket, and the client reconnects and re-reads the inbox.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use axum::extract::ws::Utf8Bytes;
use tokio::sync::mpsc;

pub const QUEUE: usize = 64;

#[derive(Default)]
pub struct Hub {
    conns: RwLock<HashMap<String, Vec<(u64, mpsc::Sender<Utf8Bytes>)>>>,
    next_id: AtomicU64,
}

#[derive(Debug, PartialEq)]
pub struct Fanout {
    pub sent: usize,
    pub dropped_slow: usize,
}

impl Hub {
    /// Registers a connection. None if the user already has `max_per_user` connections.
    pub fn register(&self, user: &str, max_per_user: usize) -> Option<(u64, mpsc::Receiver<Utf8Bytes>)> {
        let mut conns = self.conns.write().unwrap();
        let list = conns.entry(user.to_string()).or_default();
        if list.len() >= max_per_user {
            return None;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(QUEUE);
        list.push((id, tx));
        Some((id, rx))
    }

    pub fn unregister(&self, user: &str, id: u64) {
        let mut conns = self.conns.write().unwrap();
        if let Some(list) = conns.get_mut(user) {
            list.retain(|(i, _)| *i != id);
            if list.is_empty() {
                conns.remove(user);
            }
        }
    }

    /// Queues a frame for every connection of `user`.
    pub fn send_to(&self, user: &str, frame: &Utf8Bytes) -> Fanout {
        let mut out = Fanout { sent: 0, dropped_slow: 0 };
        let mut slow = vec![];
        if let Some(list) = self.conns.read().unwrap().get(user) {
            for (id, tx) in list {
                match tx.try_send(frame.clone()) {
                    Ok(()) => out.sent += 1,
                    Err(mpsc::error::TrySendError::Full(_)) => slow.push(*id),
                    Err(mpsc::error::TrySendError::Closed(_)) => {}
                }
            }
        }
        for id in slow {
            self.unregister(user, id);
            out.dropped_slow += 1;
        }
        out
    }

    /// Queues a frame for everyone (e.g. "resync" after the Redis subscription was interrupted).
    pub fn broadcast(&self, frame: &Utf8Bytes) {
        let users: Vec<String> = self.conns.read().unwrap().keys().cloned().collect();
        for u in users {
            self.send_to(&u, frame);
        }
    }

    pub fn connections(&self) -> usize {
        self.conns.read().unwrap().values().map(Vec::len).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_connections_per_user() {
        let hub = Hub::default();
        let a = hub.register("u1", 2).unwrap();
        let _b = hub.register("u1", 2).unwrap();
        assert!(hub.register("u1", 2).is_none());
        assert!(hub.register("u2", 2).is_some());
        assert_eq!(hub.connections(), 3);
        hub.unregister("u1", a.0);
        assert!(hub.register("u1", 2).is_some());
    }

    #[tokio::test]
    async fn delivers_to_every_connection_of_a_user() {
        let hub = Hub::default();
        let (_, mut r1) = hub.register("u1", 5).unwrap();
        let (_, mut r2) = hub.register("u1", 5).unwrap();
        let (_, mut other) = hub.register("u2", 5).unwrap();
        assert_eq!(hub.send_to("u1", &"hi".into()), Fanout { sent: 2, dropped_slow: 0 });
        assert_eq!(r1.recv().await.unwrap().as_str(), "hi");
        assert_eq!(r2.recv().await.unwrap().as_str(), "hi");
        assert!(other.try_recv().is_err());
        assert_eq!(hub.send_to("nobody", &"x".into()), Fanout { sent: 0, dropped_slow: 0 });
    }

    #[tokio::test]
    async fn drops_slow_clients_instead_of_waiting() {
        let hub = Hub::default();
        let (_, mut rx) = hub.register("u1", 5).unwrap();
        for _ in 0..QUEUE {
            assert_eq!(hub.send_to("u1", &"m".into()).sent, 1);
        }
        assert_eq!(hub.send_to("u1", &"m".into()), Fanout { sent: 0, dropped_slow: 1 });
        assert_eq!(hub.connections(), 0);
        // The session drains what was queued, then sees the channel closed and disconnects.
        for _ in 0..QUEUE {
            assert!(rx.recv().await.is_some());
        }
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn broadcast_reaches_everyone() {
        let hub = Hub::default();
        let (_, mut a) = hub.register("u1", 5).unwrap();
        let (_, mut b) = hub.register("u2", 5).unwrap();
        hub.broadcast(&"resync".into());
        assert_eq!(a.recv().await.unwrap().as_str(), "resync");
        assert_eq!(b.recv().await.unwrap().as_str(), "resync");
    }
}
