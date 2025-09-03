use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};

use super::AnyResult;

#[derive(Debug)]
struct HeapEntry<T> {
    item: T,
    iter_idx: usize,
}

impl<T: Ord> PartialEq for HeapEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.item.eq(&other.item) && self.iter_idx == other.iter_idx
    }
}

impl<T: Ord> Eq for HeapEntry<T> {}

impl<T: Ord> PartialOrd for HeapEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord> Ord for HeapEntry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.item.cmp(&other.item) {
            Ordering::Equal => self.iter_idx.cmp(&other.iter_idx),
            non_eq => non_eq,
        }
    }
}

/// K-way merge over `Iterator<Item = AnyResult<T>>` (T: Ord).
/// Yields ordered items; on any source `Err` the iterator returns that error and stops.
pub struct SortMergeIterator<I, T>
where
    I: Iterator<Item = AnyResult<T>>,
    T: Ord,
{
    iters: Vec<I>,
    heap: BinaryHeap<Reverse<HeapEntry<T>>>,
}

impl<I, T> SortMergeIterator<I, T>
where
    I: Iterator<Item = AnyResult<T>>,
    T: Ord,
{
    pub fn new(mut iters: Vec<I>) -> AnyResult<Self> {
        let mut heap = BinaryHeap::new();
        for (idx, iter) in iters.iter_mut().enumerate() {
            match iter.next() {
                Some(Ok(item)) => heap.push(Reverse(HeapEntry {
                    item,
                    iter_idx: idx,
                })),
                Some(Err(e)) => return Err(e),
                None => {}
            }
        }
        Ok(Self { iters, heap })
    }
}

impl<I, T> Iterator for SortMergeIterator<I, T>
where
    I: Iterator<Item = AnyResult<T>>,
    T: Ord,
{
    type Item = AnyResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let Reverse(entry) = self.heap.pop()?;
        let iter_idx = entry.iter_idx;
        let item = entry.item;

        match self.iters[iter_idx].next() {
            Some(Ok(next_item)) => {
                self.heap.push(Reverse(HeapEntry {
                    item: next_item,
                    iter_idx,
                }));
                Some(Ok(item))
            }
            Some(Err(e)) => Some(Err(e)),
            None => Some(Ok(item)),
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use super::*;

    #[test]
    fn merge_values_order_and_stability() {
        let a = vec![Ok(1), Ok(2), Ok(5)].into_iter();
        let b = vec![Ok(1), Ok(3), Ok(4)].into_iter();
        let c = vec![Ok(0), Ok(6)].into_iter();
        let mut iter = SortMergeIterator::new(vec![a, b, c]).unwrap();
        let mut out = Vec::new();
        while let Some(res) = iter.next() {
            out.push(res.unwrap());
        }
        assert_eq!(out, vec![0, 1, 1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn merge_propagates_initial_error() {
        let a = vec![Err(anyhow!("boom"))];
        let b = vec![Ok(1), Ok(2)];
        let res = SortMergeIterator::new(vec![a.into_iter(), b.into_iter()]);
        match res {
            Err(e) => assert!(e.to_string().contains("boom")),
            Ok(_) => panic!("expected error during initialization"),
        }
    }

    #[test]
    fn merge_propagates_error_during_replenish() {
        // a yields 1 then Err; b yields 2
        let a = vec![Ok(1), Err(anyhow!("err-after-1"))].into_iter();
        let b = vec![Ok(2)].into_iter();
        let mut iter = SortMergeIterator::new(vec![a, b]).unwrap();

        // Because replenish of the source producing `1` will produce an error,
        // the iterator should return that error immediately. Other sources may
        // still have buffered items which should be yielded.
        assert!(matches!(iter.next(), Some(Err(e)) if e.to_string().contains("err-after-1")));
        // next should return the remaining item from the other source (2)
        let next = iter.next();
        assert_eq!(next.unwrap().unwrap(), 2);
        assert!(iter.next().is_none());
    }
}
