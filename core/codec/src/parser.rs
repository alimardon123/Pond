// parser.rs — Zero-copy cursor over a PND2 inner-data byte slice.
//
// `PND2Parser` is a tiny helper struct that wraps a `&[u8]` and a cursor
// position, exposing little-endian readers for u8 / u16 / u32 / i64 / f64 /
// raw bytes. The decoder (`decode.rs`) uses it to walk the schema + stats +
// payload sections of a PND2 blob without copying.
//
// The struct and all its methods are `pub` so that the PyO3 wrapper crate
// can reuse the same zero-copy reader.

use crate::constants::*;

/// Zero-copy cursor over a PND2 inner-data byte slice.
pub struct PND2Parser<'a> {
    pub data: &'a [u8],
    pub pos: usize,
}

impl<'a> PND2Parser<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn read_u8(&mut self) -> Option<u8> {
        if self.pos >= self.data.len() { return None; }
        let v = self.data[self.pos];
        self.pos += 1;
        Some(v)
    }

    #[allow(dead_code)]
    pub fn read_u16(&mut self) -> Option<u16> {
        if self.pos + 2 > self.data.len() { return None; }
        let v = u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Some(v)
    }

    pub fn read_u32(&mut self) -> Option<u32> {
        if self.pos + 4 > self.data.len() { return None; }
        let v = u32::from_le_bytes([
            self.data[self.pos], self.data[self.pos + 1],
            self.data[self.pos + 2], self.data[self.pos + 3]
        ]);
        self.pos += 4;
        Some(v)
    }

    #[allow(dead_code)]
    pub fn read_i64(&mut self) -> Option<i64> {
        if self.pos + 8 > self.data.len() { return None; }
        let v = i64::from_le_bytes([
            self.data[self.pos], self.data[self.pos + 1],
            self.data[self.pos + 2], self.data[self.pos + 3],
            self.data[self.pos + 4], self.data[self.pos + 5],
            self.data[self.pos + 6], self.data[self.pos + 7]
        ]);
        self.pos += 8;
        Some(v)
    }

    #[allow(dead_code)]
    pub fn read_f64(&mut self) -> Option<f64> {
        if self.pos + 8 > self.data.len() { return None; }
        let v = f64::from_le_bytes([
            self.data[self.pos], self.data[self.pos + 1],
            self.data[self.pos + 2], self.data[self.pos + 3],
            self.data[self.pos + 4], self.data[self.pos + 5],
            self.data[self.pos + 6], self.data[self.pos + 7]
        ]);
        self.pos += 8;
        Some(v)
    }

    pub fn read_bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        if self.pos + len > self.data.len() { return None; }
        let v = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Some(v)
    }

    pub fn skip_stat_value(&mut self, vtype: u8) {
        match vtype {
            VT_INT64 | VT_FLOAT64 | VT_BOOLEAN | VT_DATE | VT_TIMESTAMP => { self.pos += 8; }
            VT_STRING | VT_BINARY => {
                if let Some(len) = self.read_u32() {
                    self.pos += len as usize;
                }
            }
            VT_NULL => {}
            _ => {}
        }
    }
}
