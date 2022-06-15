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
/// Events are bundled together and only printed after the enclosing span exits.
/// This allows to print information at the tpo that is only available later on.
///
/// Note: Type used as key in `AnyMap` inside span extensions.
struct OutputBuffer(Vec<BufferedLine>);

/// Formatted but not-yet printed output line.
struct BufferedLine {
    indent: usize,
    output_line: String,
}

/// Information added to a span through events happening within.
struct SpanInfo(Vec<String>);

impl<S: Subscriber + for<'span> LookupSpan<'span>> Layer<S> for IoTraceLayer {
    fn on_enter(&self, id: &span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let span = ctx.span(id).unwrap();
        span.extensions_mut().replace(OutputBuffer(vec![]));
        span.extensions_mut().replace(SpanInfo(vec![]));
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut visitor = IoEventVisitor::default();
        event.record(&mut visitor);

         match visitor.t {
            Some(IoEventType::DbOp(db_op)) => {
                let col = visitor.col.as_deref().unwrap_or("?");
                let key = visitor.key.as_deref().unwrap_or("?");
                let size = visitor.size.map(|num| num.to_string());
                let formatted_size = size.as_deref().unwrap_or("-");let output_line =
                format!("{db_op} {col} {key:?} size={formatted_size}");
                if let Some(span) = ctx.event_span(event) {
                    span.extensions_mut().get_mut::<OutputBuffer>().unwrap().0.push(BufferedLine { indent: 2, output_line });
                    
                } else {
                    // Print top level unbuffered.
                    writeln!(self.file.lock().unwrap(), "{output_line}").unwrap();
                }

            },
            Some(IoEventType::StorageOp(storage_op)) => {
                let key = visitor.key.as_deref().unwrap_or("?");
                let size = visitor.size.map(|num| num.to_string());
                let formatted_size = size.as_deref().unwrap_or("-");
                let tn_db_reads = visitor.tn_db_reads.unwrap();
                let tn_mem_reads = visitor.tn_mem_reads.unwrap();

                let span_info = 
                format!("{storage_op} key={key} size={formatted_size} tn_db_reads={tn_db_reads} tn_mem_reads={tn_mem_reads}");
                
                let span = ctx.event_span(event).expect("storage operations must happen inside span");
                    span.extensions_mut().get_mut::<SpanInfo>().unwrap().0.push(span_info);
            }
            None => {
                // Ignore irrelevant tracing events.
                return;
            }
        }
    }

    fn on_exit(&self, id: &span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let span = ctx.span(id).unwrap();
        let name = span.name();
        let span_line = {
            let span_info = span.extensions_mut().replace(SpanInfo(vec![])).unwrap();
            format!("{name} {}", span_info.0.join(" "))
        };

        let OutputBuffer(mut exiting_buffer) =
            span.extensions_mut().replace(OutputBuffer(vec![])).unwrap();

        if let Some(parent) = span.parent() {
            let mut ext = parent.extensions_mut();
            let OutputBuffer(parent_buffer) = ext.get_mut().unwrap();
            parent_buffer.push(BufferedLine { indent: 2, output_line: span_line });
            parent_buffer.extend(exiting_buffer.drain(..).map(|mut line| {
                line.indent += 2;
                line
            }));
        } else {
            let mut out = self.file.lock().unwrap();
            writeln!(out, "{span_line}").unwrap();
            for BufferedLine { indent, output_line } in exiting_buffer.drain(..) {
                writeln!(out, "{:indent$}{output_line}", "").unwrap();
            }
        }
    }

    fn on_close(&self, _id: span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {}
}

impl IoTraceLayer {
    pub fn new(file: Mutex<File>) -> Self {
        Self { file }
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
