extern crate crossbeam_epoch;
extern crate crossbeam_utils;

pub mod cowcell;
// pub mod ebrcell;
pub mod bst;

use std::ops::{BitAnd, BitOr};
use std::fmt;
use std::iter::FromIterator;
use std::cmp::Ordering;

pub trait AndNot<RHS = Self> {
    type Output;
    fn andnot(self, rhs: RHS) -> Self::Output;
}

pub trait IDL {
    fn push_id(&mut self, value: u64);
    fn len(&self) -> usize;
}

#[derive(Debug, PartialEq)]
pub struct IDLSimple(Vec<u64>);

impl IDLSimple {
    pub fn new() -> Self {
        IDLSimple(Vec::with_capacity(128))
    }

    pub fn from_u64(id: u64) -> Self {
        let mut new = IDLSimple::new();
        new.push_id(id);
        new
    }

    fn bstbitand(&self, candidate: &u64) -> Self {
        let mut result = IDLSimple::new();
        if let Ok(_idx) = self.0.binary_search(candidate) {
            result.0.push(*candidate);
        };
        result
    }
}

impl IDL for IDLSimple {
    fn push_id(&mut self, value: u64) {
        let &mut IDLSimple(ref mut list) = self;
        list.push(value)
    }

    fn len(&self) -> usize {
        let &IDLSimple(ref list) = self;
        list.len()
    }

}

impl FromIterator<u64> for IDLSimple {
    fn from_iter<I: IntoIterator<Item=u64>>(iter: I) -> Self {
        let mut list = Vec::with_capacity(8);
        for i in iter {
            list.push(i);
        }
        IDLSimple(list)
    }
}

#[derive(Debug)]
pub struct IDLSimpleIter<'b> {
    simpleiter: std::slice::Iter<'b, u64>,
}

impl<'b> Iterator for IDLSimpleIter<'b> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        if let Some(id) = self.simpleiter.next() {
            Some(id.clone())
        } else {
            None
        }
    }
}

impl<'b> IntoIterator for &'b IDLSimple {
    type Item = u64;
    type IntoIter = IDLSimpleIter<'b>;

    fn into_iter(self) -> Self::IntoIter {
        IDLSimpleIter {
            simpleiter: (&self.0).into_iter(),
        }
    }
}

impl BitAnd for IDLSimple
{
    type Output = Self;

    fn bitand(self, other: Self) -> Self {

        if self.0.len() == 1 {
            return other.bstbitand(self.0.first().unwrap());
        } else if other.0.len() == 1 {
            return self.bstbitand(other.0.first().unwrap());
        }

        let IDLSimple(rhs) = other;
        let IDLSimple(lhs) = self;

        let mut result = IDLSimple::new();

        let mut liter = lhs.iter();
        let mut riter = rhs.iter();

        let mut lnext = liter.next();
        let mut rnext = riter.next();

        while lnext.is_some() && rnext.is_some() {
            let l = lnext.unwrap();
            let r = rnext.unwrap();

            if l == r {
                // result.push_id(l.clone());
                result.push_id(*l);
                lnext = liter.next();
                rnext = riter.next();
            } else if l < r {
                lnext = liter.next();
            } else {
                rnext = riter.next();
            }

        }
        result

    }
}

impl BitOr for IDLSimple
{
    type Output = Self;

    fn bitor(self, IDLSimple(rhs): Self) -> Self {
        let IDLSimple(lhs) = self;
        let mut result = IDLSimple::new();

        let mut liter = lhs.iter();
        let mut riter = rhs.iter();

        let mut lnext = liter.next();
        let mut rnext = riter.next();

        while lnext.is_some() && rnext.is_some() {
            let l = lnext.unwrap();
            let r = rnext.unwrap();

            let n = if l == r {
                lnext = liter.next();
                rnext = riter.next();
                l
            } else if l < r {
                lnext = liter.next();
                l
            } else {
                rnext = riter.next();
                r
            };
            result.push_id(n.clone());

        };

        while lnext.is_some() {
            let l = lnext.unwrap();
            result.push_id(l.clone());
            lnext = liter.next();
        }

        while rnext.is_some() {
            let r = rnext.unwrap();
            result.push_id(r.clone());
            rnext = riter.next();
        }
        result
    }
}

impl AndNot for IDLSimple {
    type Output = Self;

    fn andnot(self, IDLSimple(rhs): Self) -> Self {
        let IDLSimple(lhs) = self;
        let mut result = IDLSimple::new();

        /*  LEFT is the a not b, IE a - b set wise. */
        let mut liter = lhs.iter();
        let mut riter = rhs.iter();

        let mut lnext = liter.next();
        let mut rnext = riter.next();

        while lnext.is_some() && rnext.is_some() {
            let l = lnext.unwrap();
            let r = rnext.unwrap();

            if l < r {
                result.push_id(l.clone());
                lnext = liter.next();
            } else if l == r {
                lnext = liter.next();
                rnext = riter.next();
            } else if l > r {
                rnext = riter.next();
            }

        };

        /* Push the remaining A set elements. */
        while lnext.is_some() {
            let l = lnext.unwrap();
            result.push_id(l.clone());
            lnext = liter.next();
        }

        result
    }
}

#[derive(Debug)]
struct IDLRange {
    range: u64,
    mask: u64,
}

// To make binary search, Ord only applies to range.

impl Ord for IDLRange {
    fn cmp(&self, other: &Self) -> Ordering {
        self.range.cmp(&other.range)
    }
}

impl PartialOrd for IDLRange {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for IDLRange {
    fn eq(&self, other: &Self) -> bool {
        self.range == other.range && self.mask == other.mask
    }
}

impl Eq for IDLRange {}

impl IDLRange {
    fn new(range: u64, mask: u64) -> Self {
        IDLRange {
            range: range,
            mask: mask,
        }
    }

    fn push_id(&mut self, value: u64) {
        let nmask = 1 << value;
        self.mask ^= nmask;
    }
}

#[derive(PartialEq)]
pub struct IDLBitRange {
    list: Vec<IDLRange>,
}

impl IDLBitRange {
    fn new() -> Self {
        IDLBitRange {
            list: Vec::new(),
        }
    }

    pub fn from_u64(id: u64) -> Self {
        let mut new = IDLBitRange::new();
        new.push_id(id);
        new
    }

    fn bstbitand(&self, candidate: &IDLRange) -> Self {
        let mut result = IDLBitRange::new();
        if let Ok(idx) = self.list.binary_search(candidate) {
            let existing = self.list.get(idx).unwrap();
            let mask = existing.mask & candidate.mask;
            if mask > 0 {
                let newrange = IDLRange::new(candidate.range, mask);
                result.list.push(newrange);
            };
        };
        result
    }
}

impl IDL for IDLBitRange {
    fn push_id(&mut self, value: u64) {
        // Get what range this should be
        let bvalue: u64 = value % 64;
        let range: u64 = value - bvalue;

        // Get the highest IDLRange out:
        if let Some(last) = self.list.last_mut() {
            if (*last).range == range {
                // Insert the bit.
                (*last).push_id(bvalue);
                return;
            }
        }

        // New takes a starting mask, not a raw bval, so shift it!
        let newrange = IDLRange::new(range, 1 << bvalue);
        self.list.push(newrange);
    }

    fn len(&self) -> usize {
        0
        // Right now, this would require a complete walk of the bitmask.
        // self.count
    }
}

impl FromIterator<u64> for IDLBitRange {
    fn from_iter<I: IntoIterator<Item=u64>>(iter: I) -> Self {
        let mut new = IDLBitRange {
            list: Vec::new(),
            // count: 0,
        };
        for i in iter {
            new.push_id(i);
        }
        new
    }
}

impl BitAnd for IDLBitRange
{
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        /*
         * If one candidate range has only a single range,
         * we can do a much faster search / return.
         */
        /*
         * lkrispen: comment out unless implemented for IDLsimple
         * wibrown: Well, this doesn't exist today on IDL, so it's
         * fair to take any improvement we can :) But I'll add it
         * to IDL simple anyway.
         */
        if self.list.len() == 1 {
            return rhs.bstbitand(self.list.first().unwrap());
        } else if rhs.list.len() == 1 {
            return self.bstbitand(rhs.list.first().unwrap());
        }

        let mut result = IDLBitRange::new();

        let mut liter = self.list.iter();
        let mut riter = rhs.list.iter();

        let mut lnextrange = liter.next();
        let mut rnextrange = riter.next();

        while lnextrange.is_some() && rnextrange.is_some() {
            let l = lnextrange.unwrap();
            let r = rnextrange.unwrap();

            if l.range == r.range {
                let mask = l.mask & r.mask;
                if mask > 0 {
                    let newrange = IDLRange::new(l.range, mask);
                    result.list.push(newrange);
                }
                lnextrange = liter.next();
                rnextrange = riter.next();
            } else if l.range < r.range {
                lnextrange = liter.next();
            } else {
                rnextrange = riter.next();
            }

        }
        result
    }
}

impl BitOr for IDLBitRange
{
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        let mut result = IDLBitRange::new();

        let mut liter = self.list.iter();
        let mut riter = rhs.list.iter();

        let mut lnextrange = liter.next();
        let mut rnextrange = riter.next();

        while lnextrange.is_some() && rnextrange.is_some() {
            let l = lnextrange.unwrap();
            let r = rnextrange.unwrap();

            let (range, mask) = if l.range == r.range {
                lnextrange = liter.next();
                rnextrange = riter.next();
                (l.range, l.mask | r.mask)
            } else if l.range < r.range {
                lnextrange = liter.next();
                (l.range, l.mask)
            } else {
                rnextrange = riter.next();
                (r.range, r.mask)
            };
            let newrange = IDLRange::new(range, mask);
            result.list.push(newrange);
        }

        while lnextrange.is_some() {
            let l = lnextrange.unwrap();

            let newrange = IDLRange::new(l.range, l.mask);
            result.list.push(newrange);
            lnextrange = liter.next();
        }

        while rnextrange.is_some() {
            let r = rnextrange.unwrap();

            let newrange = IDLRange::new(r.range, r.mask);
            result.list.push(newrange);
            rnextrange = riter.next();
        }
        result
    }
}

impl AndNot for IDLBitRange {
    type Output = Self;

    fn andnot(self, rhs: Self) -> Self {
        let mut result = IDLBitRange::new();

        let mut liter = self.list.iter();
        let mut riter = rhs.list.iter();

        let mut lnextrange = liter.next();
        let mut rnextrange = riter.next();

        while lnextrange.is_some() && rnextrange.is_some() {
            let l = lnextrange.unwrap();
            let r = rnextrange.unwrap();

            if l.range == r.range {
                let mask = l.mask & (!r.mask);
                if mask > 0 {
                    let newrange = IDLRange::new(l.range, mask);
                    result.list.push(newrange);
                }
                lnextrange = liter.next();
                rnextrange = riter.next();
            } else if l.range < r.range {
                lnextrange = liter.next();
            } else {
                rnextrange = riter.next();
            }
        }

        while lnextrange.is_some() {
            let l = lnextrange.unwrap();

            let newrange = IDLRange::new(l.range, l.mask);
            result.list.push(newrange);
            lnextrange = liter.next();
        }
        result
    }
}

#[derive(Debug)]
pub struct IDLBitRangeIter<'a> {
    // rangeiter: std::vec::IntoIter<IDLRange>,
    rangeiter: std::slice::Iter<'a, IDLRange>,
    currange: Option<&'a IDLRange>,
    curbit: u64,
}

impl<'a>Iterator for IDLBitRangeIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        while self.currange.is_some() {
            let range = self.currange.unwrap();
            while self.curbit < 64 {
                let m: u64 = 1 << self.curbit;
                let candidate: u64 = range.mask & m;
                if candidate > 0 {
                    let result = Some(self.curbit + range.range);
                    self.curbit += 1;
                    return result;
                }
                self.curbit += 1;
            }
            self.currange = self.rangeiter.next();
            self.curbit = 0;
        }
        None
    }
}

impl<'a> IntoIterator for &'a IDLBitRange {
    type Item = u64;
    type IntoIter = IDLBitRangeIter<'a>;

    fn into_iter(self) -> IDLBitRangeIter<'a> {
        let mut liter = (&self.list).into_iter();
        let nrange = liter.next();
        IDLBitRangeIter {
            rangeiter: liter,
            currange: nrange,
            curbit: 0,
        }
    }
}

impl fmt::Debug for IDLBitRange {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "IDLBitRange (compressed) {:?} (decompressed) [ ", self.list).unwrap();
        for id in self {
            write!(f, "{}, ", id).unwrap();
        }
        write!(f, "]")
    }
}



#[cfg(test)]
mod tests {
    // use test::Bencher;
    use super::{IDLSimple, IDLBitRange, AndNot};
    use std::iter::FromIterator;

    #[test]
    fn test_simple_intersection_1() {
        let idl_a = IDLSimple::from_iter(vec![1, 2, 3]);
        let idl_b = IDLSimple::from_iter(vec![2]);
        let idl_expect = IDLSimple::from_iter(vec![2]);

        let idl_result = idl_a & idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_range_intersection_1() {
        let idl_a = IDLBitRange::from_iter(vec![1, 2, 3]);
        let idl_b = IDLBitRange::from_iter(vec![2]);
        let idl_expect = IDLBitRange::from_iter(vec![2]);

        let idl_result = idl_a & idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_simple_intersection_2() {
        let idl_a = IDLSimple::from_iter(vec![1, 2, 3]);
        let idl_b = IDLSimple::from_iter(vec![4, 67]);
        let idl_expect = IDLSimple::new();

        let idl_result = idl_a & idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_range_intersection_2() {
        let idl_a = IDLBitRange::from_iter(vec![1, 2, 3]);
        let idl_b = IDLBitRange::from_iter(vec![4, 67]);
        let idl_expect = IDLBitRange::new();


        let idl_result = idl_a & idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_range_intersection_3() {
        let idl_a = IDLBitRange::from_iter(vec![1, 2, 3, 4, 35, 64, 65, 128, 150]);
        let idl_b = IDLBitRange::from_iter(vec![2, 3, 8, 35, 64, 128, 130, 150, 152, 180]);
        let idl_expect = IDLBitRange::from_iter(vec![2, 3, 35, 64, 128, 150]);

        let idl_result = idl_a & idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_simple_intersection_4() {
        let idl_a = IDLSimple::from_iter(vec![2, 3, 8, 35, 64, 128, 130, 150, 152, 180, 256, 800, 900]);
        let idl_b = IDLSimple::from_iter(1..1024);
        let idl_expect = IDLSimple::from_iter(vec![2, 3, 8, 35, 64, 128, 130, 150, 152, 180, 256, 800, 900]);

        let idl_result = idl_a & idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_range_intersection_4() {
        let idl_a = IDLBitRange::from_iter(vec![2, 3, 8, 35, 64, 128, 130, 150, 152, 180, 256, 800, 900]);
        let idl_b = IDLBitRange::from_iter(1..1024);
        let idl_expect = IDLBitRange::from_iter(vec![2, 3, 8, 35, 64, 128, 130, 150, 152, 180, 256, 800, 900]);

        let idl_result = idl_a & idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_simple_intersection_5() {
        let idl_a = IDLSimple::from_iter(1..204800);
        let idl_b = IDLSimple::from_iter(102400..307200);
        let idl_expect = IDLSimple::from_iter(102400..204800);

        let idl_result = idl_a & idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_range_intersection_5() {
        let idl_a = IDLBitRange::from_iter(1..204800);
        let idl_b = IDLBitRange::from_iter(102400..307200);
        let idl_expect = IDLBitRange::from_iter(102400..204800);

        let idl_result = idl_a & idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_simple_intersection_6() {
        let idl_a = IDLSimple::from_iter(vec![307199]);
        let idl_b = IDLSimple::from_iter(102400..307200);
        let idl_expect = IDLSimple::from_iter(vec![307199]);

        let idl_result = idl_a & idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_range_intersection_6() {
        let idl_a = IDLBitRange::from_iter(vec![307199]);
        let idl_b = IDLBitRange::from_iter(102400..307200);
        let idl_expect = IDLBitRange::from_iter(vec![307199]);

        let idl_result = idl_a & idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_simple_union_1() {
        let idl_a = IDLSimple::from_iter(vec![1,2,3]);
        let idl_b = IDLSimple::from_iter(vec![2]);
        let idl_expect = IDLSimple::from_iter(vec![1,2,3]);

        let idl_result = idl_a | idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_range_union_1() {
        let idl_a = IDLBitRange::from_iter(vec![1,2,3]);
        let idl_b = IDLBitRange::from_iter(vec![2]);
        let idl_expect = IDLBitRange::from_iter(vec![1, 2, 3]);

        let idl_result = idl_a | idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_simple_union_2() {
        let idl_a = IDLSimple::from_iter(vec![1,2,3]);
        let idl_b = IDLSimple::from_iter(vec![4,67]);
        let idl_expect = IDLSimple::from_iter(vec![1,2,3,4,67]);

        let idl_result = idl_a | idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_range_union_2() {
        let idl_a = IDLBitRange::from_iter(vec![1,2,3]);
        let idl_b = IDLBitRange::from_iter(vec![4,67]);
        let idl_expect = IDLBitRange::from_iter(vec![1,2,3,4,67]);

        let idl_result = idl_a | idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_simple_union_3() {
        let idl_a = IDLSimple::from_iter(vec![2, 3, 8, 35, 64, 128, 130, 150, 152, 180, 256, 800, 900]);
        let idl_b = IDLSimple::from_iter(1..1024);
        let idl_expect = IDLSimple::from_iter(1..1024);

        let idl_result = idl_a | idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_range_union_3() {
        let idl_a = IDLBitRange::from_iter(vec![2, 3, 8, 35, 64, 128, 130, 150, 152, 180, 256, 800, 900]);
        let idl_b = IDLBitRange::from_iter(1..1024);
        let idl_expect = IDLBitRange::from_iter(1..1024);

        let idl_result = idl_a | idl_b;
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_simple_not_1() {
        let idl_a = IDLSimple::from_iter(vec![1,2,3,4,5,6]);
        let idl_b = IDLSimple::from_iter(vec![3,4]);
        let idl_expect = IDLSimple::from_iter(vec![1,2,5,6]);

        let idl_result = idl_a.andnot(idl_b);
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_simple_not_2() {
        let idl_a = IDLSimple::from_iter(vec![1,2,3,4,5,6]);
        let idl_b = IDLSimple::from_iter(vec![10]);
        let idl_expect = IDLSimple::from_iter(vec![1,2,3,4,5,6]);

        let idl_result = idl_a.andnot(idl_b);
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_simple_not_3() {
        let idl_a = IDLSimple::from_iter(vec![2,3,4,5,6]);
        let idl_b = IDLSimple::from_iter(vec![1]);
        let idl_expect = IDLSimple::from_iter(vec![2,3,4,5,6]);

        let idl_result = idl_a.andnot(idl_b);
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_range_not_1() {
        let idl_a = IDLBitRange::from_iter(vec![1,2,3,4,5,6]);
        let idl_b = IDLBitRange::from_iter(vec![3,4]);
        let idl_expect = IDLBitRange::from_iter(vec![1,2,5,6]);

        let idl_result = idl_a.andnot(idl_b);
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_range_not_2() {
        let idl_a = IDLBitRange::from_iter(vec![1,2,3,4,5,6]);
        let idl_b = IDLBitRange::from_iter(vec![10]);
        let idl_expect = IDLBitRange::from_iter(vec![1,2,3,4,5,6]);

        let idl_result = idl_a.andnot(idl_b);
        assert_eq!(idl_result, idl_expect);
    }

    #[test]
    fn test_range_not_3() {
        let idl_a = IDLBitRange::from_iter(vec![2,3,4,5,6]);
        let idl_b = IDLBitRange::from_iter(vec![1]);
        let idl_expect = IDLBitRange::from_iter(vec![2,3,4,5,6]);

        let idl_result = idl_a.andnot(idl_b);
        assert_eq!(idl_result, idl_expect);
    }

    /*
    #[bench]
    fn bench_range_intersection_1(b: &mut Bencher) {
        b.iter(|| {
            let mut idl_a = IDLBitRange::new();
            let mut idl_b = IDLBitRange::new();
            test_dataset_4(&mut idl_a, &mut idl_b);
            idl_a & idl_b
        });
    }
    */
}

