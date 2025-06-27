#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use tracing::{Event, Subscriber, dispatcher::DefaultGuard};
use tracing_subscriber::{Layer, Registry, layer::Context, prelude::*};

/// A `tracing` [`Layer`] that records the `path` field of every event whose
/// target is `parser.trace_point`.
struct PathRecorder {
    paths: Arc<Mutex<Vec<String>>>,
}

impl<S> Layer<S> for PathRecorder
where S: Subscriber
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // Only consider events with our dedicated target.
        if event.metadata().target() != "parser.trace_point" {
            return;
        }
        // Extract the "path" field value.
        let mut visitor = PathVisitor::default();
        event.record(&mut visitor);
        if let Some(path) = visitor.path {
            if let Ok(mut vec) = self.paths.lock() {
                vec.push(path);
            }
        }
    }
}

/// Visitor to capture the `path` field from an event.
#[derive(Default)]
struct PathVisitor {
    path: Option<String>,
}

impl tracing::field::Visit for PathVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "path" {
            self.path = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}

/// Guard returned by [`capture`]. When dropped, it unregisters the tracing
/// layer and owns its recorded paths.
pub struct TraceGuard {
    _guard: DefaultGuard,
    paths: Arc<Mutex<Vec<String>>>,
}

impl TraceGuard {
    /// Returns `true` if a given path was captured.
    pub fn hit(&self, target_path: &str) -> bool {
        if let Ok(vec) = self.paths.lock() {
            vec.iter().any(|p| p == target_path)
        } else {
            false
        }
    }

    /// Returns all captured paths (primarily for debugging).
    pub fn collected(&self) -> Vec<String> {
        self.paths.lock().map(|v| v.clone()).unwrap_or_default()
    }
}

/// Install a temporary [`tracing`] layer that captures every `parser.trace_point`
/// event's `path` value.
///
/// The returned [`TraceGuard`] must be held for the lifespan you wish to record
/// events. Dropping it automatically disables the layer and releases the data.
pub fn capture() -> TraceGuard {
    let paths = Arc::new(Mutex::new(Vec::new()));

    // Build a subscriber with our custom layer.
    let subscriber = Registry::default().with(PathRecorder {
        paths: paths.clone(),
    });

    // Install it as the default for the current thread.
    let guard = tracing::subscriber::set_default(subscriber);

    TraceGuard {
        _guard: guard,
        paths,
    }
}
