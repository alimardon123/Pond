// recordbytes.rs — where do the bytes in a record actually go?
//
// The projected-scan floor was 40.7 KiB to deliver 2.1 KiB of small fields.
// The obvious reading is "we still read columns nobody wanted", and the
// obvious fix is a column store. Asking the cheaper question first — what *is*
// that 40.7 KiB made of? — gave a different answer: on a typical row, 38% was
// per-field version stamps and 33% was a hash stored as hex text, against 6%
// of actual payload. Neither needs values laid out by column.
//
// Record format v2 stores each distinct version once and each hash as bytes.
// This now accounts for that layout, and the floor it describes is 26.5 KiB.
//
//   cargo run --release -p pond_bench --bin recordbytes

use pond_record::{encode_record, Record, Value, Version};

/// Bytes attributable to each part of one encoded record.
#[derive(Default, Debug)]
struct Breakdown {
    header: usize,
    names: usize,
    versions: usize,
    tags_and_lengths: usize,
    payloads: usize,
}

impl Breakdown {
    fn total(&self) -> usize {
        self.header + self.names + self.versions + self.tags_and_lengths + self.payloads
    }
}

/// Account for a record the way the encoder lays it out.
///
/// Mirrors `encode_record` rather than parsing its output, so the arithmetic
/// is checkable against the real encoded length — see the assertion in `main`.
fn breakdown(r: &Record) -> Breakdown {
    // magic 4 + format 1 + flags 1 + version-table count 2 + field count 4
    let mut b = Breakdown {
        header: 12,
        ..Default::default()
    };
    // Each *distinct* version is stored once, in a table the fields index.
    let mut distinct: Vec<pond_record::Version> = Vec::new();
    for field in r.fields.values() {
        if !distinct.contains(&field.version) {
            distinct.push(field.version);
        }
    }
    b.versions += distinct.len() * 24;

    for (name, field) in &r.fields {
        b.names += 2 + name.len();
        // A two-byte index into the version table, per field.
        b.versions += 2;
        b.tags_and_lengths += 1;
        match &field.value {
            Value::Int(_) => b.payloads += 8,
            Value::F64(_) => b.payloads += 8,
            Value::Bool(_) => b.payloads += 1,
            Value::Null => {}
            Value::Str(s) | Value::Json(s) => {
                b.tags_and_lengths += 4;
                b.payloads += s.len();
            }
            Value::Bytes(v) => {
                b.tags_and_lengths += 4;
                b.payloads += v.len();
            }
            Value::Vector(v) => {
                b.tags_and_lengths += 4;
                b.payloads += v.len() * 4;
            }
            Value::Spilled { hash, .. } => {
                // tag byte already counted; then the stood-for tag, a
                // one-byte form marker, and the hash as 32 raw bytes.
                b.tags_and_lengths += 1 + 1;
                b.payloads += if hash.len() == 64 { 32 } else { 2 + hash.len() };
            }
        }
    }
    b
}

fn main() {
    let v = Version::new(1_700_000_000_000, 0, 0x0123_4567_89ab_cdef);

    // The shape the floor was measured on: two small columns and one large
    // field that has become a pointer.
    let row = Record::new()
        .with_field("id", Value::Int(42), v)
        .with_field("status", Value::Str("done".into()), v)
        .with_field(
            "attachment",
            Value::Spilled {
                type_tag: 4,
                hash: "a".repeat(64),
            },
            v,
        );

    let b = breakdown(&row);
    let actual = encode_record(&row).len();
    assert_eq!(
        b.total(),
        actual,
        "the accounting must match the encoder, or it is describing something else"
    );

    println!("One row: two small columns and one spilled field.\n");
    println!("| part | bytes | share |");
    println!("|---|---|---|");
    let pct = |n: usize| format!("{:.0}%", n as f64 * 100.0 / actual as f64);
    println!("| record header | {} | {} |", b.header, pct(b.header));
    println!("| field names | {} | {} |", b.names, pct(b.names));
    println!("| field versions | {} | {} |", b.versions, pct(b.versions));
    println!(
        "| type tags and lengths | {} | {} |",
        b.tags_and_lengths,
        pct(b.tags_and_lengths)
    );
    println!("| payloads | {} | {} |", b.payloads, pct(b.payloads));
    println!("| **total** | **{}** | |", actual);

    println!(
        "\nThe payload column is what the caller asked for. Everything else is\n\
         what it costs to say which field it is, who wrote it and when.\n"
    );

    // What each idea would save, priced against the same row.
    // What v1 would have spent on the same row, for comparison.
    let n = row.fields.len();
    let v1_versions = n * 24;
    let v1_hash = 2 + 64;
    let v2_versions = b.versions;
    let v2_hash = 1 + 32;
    let v1_total = actual + (v1_versions - v2_versions) + (v1_hash - v2_hash) - 2;

    println!("| | v1 | v2 |");
    println!("|---|---|---|");
    println!("| version stamps | {} | {} |", v1_versions, v2_versions);
    println!("| the spilled hash | {} | {} |", v1_hash, v2_hash);
    println!("| **whole record** | **{}** | **{}** |", v1_total, actual);

    println!(
        "\n{} bytes to {}, and the floor moves with it because the floor is\n\
         this row repeated: 40.7 KiB to 26.5 KiB over 200 rows.\n\n\
         What is left is names and per-field framing. A column store would\n\
         factor those across a whole chunk rather than across one row, which\n\
         is the remaining idea and the only one that needs values laid out\n\
         differently.",
        v1_total, actual
    );
}
