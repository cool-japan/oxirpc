//! Lightweight synchronous cancellation token.
//!
//! [`CancellationToken`] is a cloneable handle to a shared `AtomicBool` flag.
//! Cancelling one handle is immediately visible to all clones (children).
//! Async `wait` is intentionally omitted to avoid a tokio dependency; callers
//! must poll [`CancellationToken::is_cancelled`] at appropriate yield points.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// A cloneable, thread-safe cancellation signal.
#[derive(Clone, Debug)]
pub struct CancellationToken {
    flag: Arc<AtomicBool>,
}

impl CancellationToken {
    /// Create a new, uncancelled token.
    pub fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Signal cancellation. All clones of this token will observe the change.
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::Release);
    }

    /// Whether cancellation has been signalled.
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    /// Return a child token that shares the same cancellation flag.
    ///
    /// Cancelling either the parent or any child cancels all of them, since they
    /// all point at the same `Arc<AtomicBool>`.
    pub fn child(&self) -> Self {
        Self {
            flag: Arc::clone(&self.flag),
        }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}
