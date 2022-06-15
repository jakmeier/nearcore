use std::io::Write;
use std::{fs::File, sync::Mutex};
use tracing::{span, Subscriber};
use tracing_subscriber::{registry::LookupSpan, Layer};

/// Tracing layer that produces a record of IO operations.
pub struct IoTraceLayer {
    file: Mutex<File>,
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

/// Formatted but not-yet printed output lines.
///
/// Some operations are bundled together and only printed after the enclosing
/// span exits. This allows to print span information before the operations that
/// happen within.
///
/// Note: Type used as key in `AnyMap` inside span extensions.
struct OutputBuffer(Vec<String>);

/// Keeps track of current indentation when printing output.
///
/// Note: Type used as key in `AnyMap` inside span extensions.
struct IndentationDepth(usize);

impl<S: Subscriber + for<'span> LookupSpan<'span>> Layer<S> for IoTraceLayer {
    fn on_enter(&self, id: &span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let span = ctx.span(id).unwrap();
        let name = span.name();
        let indent = if span.parent().is_none() {
            0
        } else {
            span.extensions().get::<IndentationDepth>().unwrap().0
        };

        // Most spans are written out directly, since they are only printed to
        // display control-flow progress in the output. Storage related host
        // functions are more important. They have the key and the value size
        // printed in the opening line. But the size is only available later.
        // Therefore, output within those spans is buffered. Note that one layer
        // of buffering is enough because no spans are created deeper inside
        // those host functions.
        match name {
            "storage_read" | "storage_write" | "storage_remove" | "storage_has_key" => {
                span.extensions_mut().replace(OutputBuffer(vec![]));
            }
            _ => {
                writeln!(self.file.lock().unwrap(), "{:indent$}{name}", "").unwrap();
            }
        }

        let new_depth = IndentationDepth(indent + 2);
        span.extensions_mut().replace(new_depth);
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut visitor = IoEventVisitor::default();
        event.record(&mut visitor);

        let indent = ctx
            .event_span(event)
            .and_then(|span| span.extensions().get::<IndentationDepth>().map(|d| d.0))
            .unwrap_or(0);

        match visitor.t {
            Some(IoEventType::DbOp(db_op)) => {
                let col = visitor.col.as_deref().unwrap_or("?");
                let key = visitor.key.as_deref().unwrap_or("?");
                let size = visitor.size.map(|num| num.to_string());
                let formatted_size = size.as_deref().unwrap_or("-");
                let output_line = format!("{db_op} {col} {key:?} size={formatted_size}");

                if let Some(span) = ctx.event_span(event) {
                    if let Some(OutputBuffer(stack)) = span.extensions_mut().get_mut() {
                        stack.push(output_line);
                        return;
                    }
                }

                writeln!(self.file.lock().unwrap(), "{:indent$}{output_line}", "").unwrap();
            }
            Some(IoEventType::StorageOp(storage_op)) => {
                let key = visitor.key.as_deref().unwrap_or("?");
                let size = visitor.size.map(|num| num.to_string());
                let formatted_size = size.as_deref().unwrap_or("-");
                let tn_db_reads = visitor.tn_db_reads.unwrap();
                let tn_mem_reads = visitor.tn_mem_reads.unwrap();
                writeln!(
                    self.file.lock().unwrap(),
                    "{:indent$}{storage_op} {key:?} size={formatted_size} tn_db_reads={tn_db_reads} tn_mem_reads={tn_mem_reads}",
                    ""
                )
                .unwrap();

                let span = ctx.event_span(event).expect("must have a parent span").id();
                self.flush_output_buffer(&span, &ctx, indent + 2);
            }
            None => { /* Ignore irrelevant tracing events. */ }
        }
    }

    fn on_exit(&self, id: &span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let span = ctx.span(id).unwrap();
        span.extensions_mut().get_mut::<IndentationDepth>().unwrap().0 -= 2;
    }

    fn on_close(&self, _id: span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {}
}

impl IoTraceLayer {
    pub fn new(file: Mutex<File>) -> Self {
        Self { file }
    }

    /// Remove and print all DB operations of the current span.
    fn flush_output_buffer<S: Subscriber + for<'span> LookupSpan<'span>>(
        &self,
        id: &span::Id,
        ctx: &tracing_subscriber::layer::Context<'_, S>,
        indent: usize,
    ) {
        let span = ctx.span(id).unwrap();
        let mut ext = span.extensions_mut();
        let buffer = ext.get_mut::<OutputBuffer>().expect("span must have db op stack");
        let mut out = self.file.lock().unwrap();
        for line in buffer.0.drain(..) {
            writeln!(out, "{:indent$}{line}", "").unwrap();
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
    tn_db_reads: Option<u64>,
    tn_mem_reads: Option<u64>,
}

impl tracing::field::Visit for IoEventVisitor {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "size" => self.size = Some(value),
            "evicted_len" => self.evicted_len = Some(value),
            "tn_db_reads" => self.tn_db_reads = Some(value),
            "tn_mem_reads" => self.tn_mem_reads = Some(value),
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
