use std::io::Write;
use std::{fs::File, sync::Mutex};
use tracing::{span, Subscriber};
use tracing_subscriber::{registry::LookupSpan, Layer};

/// Tracing layer that produces a record of IO operations.
pub struct IoTraceLayer {
    file: Mutex<File>,
}

impl IoTraceLayer {
    pub fn new(file: Mutex<File>) -> Self {
        Self { file }
    }
}

type DbOpStack = Vec<DbOp>;

impl<S: Subscriber + for<'span> LookupSpan<'span>> Layer<S> for IoTraceLayer {
    fn on_new_span(
        &self,
        _attrs: &span::Attributes<'_>,
        id: &span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx.span(id).unwrap();
        span.extensions_mut().insert(DbOpStack::new());
    }

    fn on_record(
        &self,
        _span: &span::Id,
        _values: &span::Record<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut visitor = IoEventVisitor::default();

        event.record(&mut visitor);

        match visitor.t {
            Some(IoEventType::DbOp(db_op)) => {
                if let Some(span) = ctx.event_span(event) {
                    span.extensions_mut()
                        .get_mut::<DbOpStack>()
                        .expect("span must have db op stack")
                        .push(db_op);
                } else {
                    let col = visitor.col.as_deref().unwrap_or("_");
                    let key = visitor.key.as_deref().unwrap_or("?");
                    let size = visitor.size.map(|num| num.to_string());
                    let formatted_size = size.as_deref().unwrap_or("-");
                    // GET State "3t9dCaQAfpnBq1mmHmvszYZvpLSDDYc5q2sbPycDqvRmEhozbxSwtm" 75   TrieNode
                    writeln!(self.file.lock().unwrap(), "{db_op} {col} {key:?} {formatted_size}",)
                        .unwrap();
                }
            }
            Some(IoEventType::StorageOp(storage_op)) => {
                let level = 1; // TODO
                let indent = level * 2;
                let key = visitor.key.as_deref().unwrap_or("?");
                let size = visitor.size.map(|num| num.to_string());
                let formatted_size = size.as_deref().unwrap_or("-");
                // TODO: more info
                // storage_read "AQH2oEL" 29 tn_db_reads=16 tn_mem_reads=0 time=27ms
                writeln!(
                    self.file.lock().unwrap(),
                    "{:indent$}{storage_op} {key:?} {formatted_size}",
                    ""
                )
                .unwrap();

                let span = ctx.event_span(event).expect("must have a parent span").id();
                self.flush_db_ops(&span, ctx);
            }
            None => { /* Ignore irrelevant tracing events. */ }
        }
    }

    fn on_enter(&self, id: &span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let level = 0; // TODO
        let name = ctx.span(id).unwrap().name();
        let indent = level * 2;
        writeln!(self.file.lock().unwrap(), "{:indent$}{name}", "").unwrap();
    }

    fn on_exit(&self, id: &span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        self.flush_db_ops(id, ctx);
    }

    fn on_close(&self, _id: span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {}
}

impl IoTraceLayer {
    fn flush_db_ops<S: Subscriber + for<'span> LookupSpan<'span>>(
        &self,
        id: &span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx.span(id).unwrap();
        let mut ext = span.extensions_mut();
        let db_ops = ext.get_mut::<DbOpStack>().expect("span must have db op stack");
        let mut out = self.file.lock().unwrap();
        for db_op in db_ops.drain(..) {
            let level = 1; // TODO
                           // TODO: More info
            let indent = level * 2;
            writeln!(out, "{:indent$}{db_op}", "").unwrap();
        }
    }
}

/// Builder object to fill in field-by-field on traced events.
#[derive(Default)]
struct IoEventVisitor {
    t: Option<IoEventType>,
    key: Option<String>,
    col: Option<String>,
    size: Option<u64>,
    evicted_len: Option<u64>,
    trie_nodes_db: Option<u64>,
    trie_nodes_mem: Option<u64>,
}

enum IoEventType {
    StorageOp(StorageOp),
    DbOp(DbOp),
}
#[derive(strum::Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
enum StorageOp {
    Read,
    Write,
    Other,
}
#[derive(strum::Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
enum DbOp {
    Get,
    Insert,
    Set,
    UpdateRc,
    Delete,
    DeleteAll,
    Other,
}

impl tracing::field::Visit for IoEventVisitor {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "size" => self.size = Some(value),
            "evicted_len" => self.evicted_len = Some(value),
            "trie_nodes_db" => self.trie_nodes_db = Some(value),
            "trie_nodes_mem" => self.trie_nodes_mem = Some(value),
            _ => { /* Ignore other values, likely they are used in logging. */ }
        }
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if value >= 0 {
            self.record_u64(field, value as u64);
        }
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "key" => self.key = Some(value.to_owned()),
            "col" => self.col = Some(value.to_owned()),
            "storage_op" => {
                let op = match value {
                    "write" => StorageOp::Write,
                    "read" => StorageOp::Read,
                    _ => StorageOp::Other,
                };
                self.t = Some(IoEventType::StorageOp(op));
            }
            "db_op" => {
                let op = match value {
                    "get" => DbOp::Get,
                    "insert" => DbOp::Insert,
                    "set" => DbOp::Set,
                    "updaterc" => DbOp::UpdateRc,
                    "delete" => DbOp::Delete,
                    "deleteall" => DbOp::DeleteAll,
                    _ => DbOp::Other,
                };
                self.t = Some(IoEventType::DbOp(op));
            }
            _ => { /* Ignore other values, likely they are used in logging. */ }
        }
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.record_str(field, &format!("{value:?}"))
    }
}
