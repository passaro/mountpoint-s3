use std::collections::VecDeque;

use aws_sdk_s3_transfer_manager::io::AggregatedBytes;
use bytes::{Buf as _, Bytes};

#[derive(Debug, Default)]
pub struct Buffer {
    data: VecDeque<Bytes>,
    remaining: usize,
}

impl Buffer {
    pub fn new_from_aggregated_bytes(chunk_data: AggregatedBytes) -> Self {
        let remaining = chunk_data.remaining();
        Self {
            data: chunk_data.into_segments().collect(),
            remaining,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.remaining == 0
    }

    pub fn len(&self) -> usize {
        self.remaining
    }

    pub fn extend_from(&mut self, other: &mut Self, length: usize) -> usize {
        if self.is_empty() && other.len() <= length {
            std::mem::swap(self, other);
            return self.len();
        }

        let mut copied = 0;
        while copied < length {
            let next = other.consume_front(length - copied);
            if next.is_empty() {
                break;
            }
            copied += next.len();
            self.append(next);
        }
        copied
    }

    fn append(&mut self, bytes: Bytes) {
        self.remaining += bytes.len();
        self.data.push_back(bytes);
    }

    fn consume_front(&mut self, max_length: usize) -> Bytes {
        let Some(front) = self.data.front_mut() else {
            return Bytes::default();
        };
        if front.len() > max_length {
            let chunk = front.split_to(max_length);
            self.remaining -= max_length;
            chunk
        } else {
            let front = self.data.pop_front().unwrap();
            self.remaining -= front.len();
            front
        }
    }
}

impl From<Bytes> for Buffer {
    fn from(value: Bytes) -> Self {
        let remaining = value.len();
        Self {
            data: [value].into(),
            remaining,
        }
    }
}
