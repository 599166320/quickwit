use bloomfilter::Bloom;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::BytesColumn;
use tantivy::{DocId, Score, SegmentReader};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BloomFilterCollector {
    pub field_name: String,
    pub expected_items: usize,
    pub false_positive_rate: f64,
}

impl Collector for BloomFilterCollector {
    type Fruit = Bloom<Vec<u8>>;
    type Child = BloomFilterSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let column = segment_reader
            .fast_fields()
            .bytes(&self.field_name)?
            .ok_or_else(|| {
                let err_msg = format!("failed to find column `{}`", self.field_name);
                tantivy::TantivyError::InternalError(err_msg)
            })?;

        Ok(BloomFilterSegmentCollector {
            column,
            bloom: self.create_bloom_filter(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        merge_segment_fruits(segment_fruits)
    }
}

fn merge_segment_fruits(mut segment_fruits: Vec<Bloom<Vec<u8>>>) -> tantivy::Result<Bloom<Vec<u8>>> {
    BloomFilterCollector::merge_many(&segment_fruits).ok_or(tantivy::TantivyError::InternalError(
        "segment_fruits is empty".to_string(),
    ))
}

pub struct BloomFilterSegmentCollector {
    column: BytesColumn,
    bloom: Bloom<Vec<u8>>,
}

impl SegmentCollector for BloomFilterSegmentCollector {
    type Fruit = Bloom<Vec<u8>>;
    fn collect(&mut self, doc: DocId, _score: Score) {
        let term_ord = self.column.term_ords(doc).next().unwrap_or_default();
        let mut buffer = Vec::new();
        let found_term = self
            .column
            .ord_to_bytes(term_ord, &mut buffer)
            .expect("Failed to lookup column in the column term dictionary");
        debug_assert!(found_term);
        self.bloom.set(&buffer);
    }

    fn harvest(self) -> Self::Fruit {
        self.bloom
    }
}

impl BloomFilterCollector {
    pub fn fast_field_names(&self) -> HashSet<String> {
        HashSet::from_iter([self.field_name.clone()])
    }

    pub fn create_bloom_filter(&self) -> Bloom<Vec<u8>> {
        Bloom::new_for_fp_rate(self.expected_items, self.false_positive_rate)
            .expect("Failed to create bloom filter")
    }

    fn merge_blooms(a: &Bloom<Vec<u8>>, b: &Bloom<Vec<u8>>) -> Bloom<Vec<u8>> {
        let mut merged_bytes = a.to_bytes();
        for (x, y) in merged_bytes.iter_mut().zip(b.as_slice().iter()) {
            *x |= *y;
        }

        Bloom::from_bytes(merged_bytes).expect("failed to reconstruct Bloom from bytes")
    }

    pub fn merge_with_bytes(a: &mut Bloom<Vec<u8>>, bytes: Vec<u8>) {
        if bytes.len() > 0 {
            let b = Bloom::from_bytes(bytes).expect("failed to reconstruct Bloom from bytes");
            Self::merge_assign(a, &b);
        }
    }

    pub fn merge_assign(a: &mut Bloom<Vec<u8>>, b: &Bloom<Vec<u8>>) {
        let merged = Self::merge_blooms(a, b);
        *a = merged;
    }

    pub fn merge_many_bytes<'a>(bytes: impl Iterator<Item = &'a[u8]>) -> Option<Bloom<Vec<u8>>> {
        bytes
            .map(|arg0| Bloom::from_slice(arg0))
            .filter(|x| x.is_ok())
            .map(|x| x.unwrap())
            .reduce(|a, b| Self::merge_blooms(&a, &b))
    }

    pub fn merge_many(filters: &[Bloom<Vec<u8>>]) -> Option<Bloom<Vec<u8>>> {
        if filters.is_empty() {
            return None;
        }
        let first = &filters[0];
        let mut acc = first.to_bytes();

        for f in &filters[1..] {
            for (x, y) in acc.iter_mut().zip(f.as_slice().iter()) {
                *x |= *y;
            }
        }
        Some(Bloom::from_bytes(acc).expect("from_bytes failed"))
    }
}
