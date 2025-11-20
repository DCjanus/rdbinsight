use std::sync::{Arc, Mutex};

use tracing::{Event, Subscriber, dispatcher::DefaultGuard};
use tracing_subscriber::{Layer, Registry, layer::Context, prelude::*};

/// `tracing` layer implementation，记录目标为 `parser.trace_point` 的事件里 `path` 字段，方便测试判断编码分支。
struct PathRecorder {
    paths: Arc<Mutex<Vec<String>>>,
}

impl<S> Layer<S> for PathRecorder
where S: Subscriber
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != "parser.trace_point" {
            return;
        }

        let mut visitor = PathVisitor::default();
        event.record(&mut visitor);
        if let Some(path) = visitor.path
            && let Ok(mut vec) = self.paths.lock()
        {
            vec.push(path);
        }
    }
}

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

/// `TraceGuard` 负责持有注册的 tracing 订阅器，析构时自动还原。
pub struct TraceGuard {
    _guard: DefaultGuard,
    paths: Arc<Mutex<Vec<String>>>,
}

impl TraceGuard {
    pub fn hit(&self, path: &str) -> bool {
        self.paths
            .lock()
            .map(|paths| paths.iter().any(|candidate| candidate == path))
            .unwrap_or(false)
    }

    pub fn collected(&self) -> Vec<String> {
        self.paths
            .lock()
            .map(|paths| paths.clone())
            .unwrap_or_default()
    }
}

/// 安装临时 tracing 层，用于捕获 parser trace 事件。
pub fn capture() -> TraceGuard {
    let paths = Arc::new(Mutex::new(Vec::new()));
    let subscriber = Registry::default().with(PathRecorder {
        paths: paths.clone(),
    });
    let guard = tracing::subscriber::set_default(subscriber);

    TraceGuard {
        _guard: guard,
        paths,
    }
}
