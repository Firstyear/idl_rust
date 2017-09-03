
use std::ops::{BitAnd};
use std::vec::IntoIter;
use std::fmt;
use std::mem;
use std::fmt::Debug;
use std::marker::PhantomData;

trait IDL {
    fn push_id(&mut self, value: u64);
    fn len(&self) -> usize;
}

#[derive(Debug, PartialEq)]
struct IDLSimple(Vec<u64>);

impl IDLSimple {
    fn new() -> Self {
        IDLSimple(Vec::with_capacity(8))
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


impl BitAnd for IDLSimple
{
    type Output = Self;

    fn bitand(self, IDLSimple(rhs): Self) -> Self {
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
                result.push_id(l.clone());
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

#[derive(Debug, PartialEq)]
struct IDLRange {
    range: u64,
    mask: u64,
}

impl IDLRange {
    fn new(range: u64) -> Self {
        IDLRange {
            range: range,
            mask: 0,
        }
    }

    fn push_id(&mut self, value: u64) {
        let nmask = 1 << value;
        self.mask ^= nmask;
    }
}

#[derive(PartialEq)]
struct IDLBitRange {
    list: Vec<IDLRange>,
    count: usize,
}

impl IDLBitRange {
    fn new() -> Self {
        IDLBitRange {
            list: Vec::new(),
            count: 0,
        }
    }
}

impl IDL for IDLBitRange {
    fn push_id(&mut self, value: u64) {
        // Get what range this should be
        let bvalue: u64 = value % 64;
        let range: u64 = value - bvalue;

        self.count += 1;

        // Get the highest IDLRange out:
        if let Some(last) = self.list.last_mut() {
            if (*last).range == range {
                // Insert the bit.
                (*last).push_id(bvalue);
                return;
            }
        }

        let mut newrange = IDLRange::new(range);
        newrange.push_id(bvalue);
        self.list.push(newrange);
    }

    fn len(&self) -> usize {
        self.count
    }
}

impl BitAnd for IDLBitRange
{
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        let mut result = IDLBitRange::new();

        let mut liter = self.list.iter();
        let mut riter = rhs.list.iter();

        let mut lnextrange = liter.next();
        let mut rnextrange = riter.next();

        while lnextrange.is_some() && rnextrange.is_some() {
            let l = lnextrange.unwrap();
            let r = rnextrange.unwrap();

            if l.range == r.range {
                let mut newrange = IDLRange::new(l.range);
                newrange.mask = l.mask & r.mask;
                result.list.push(newrange);
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

#[derive(Debug)]
struct IDLBitRangeIter<'a> {
    // rangeiter: std::vec::IntoIter<IDLRange>,
    rangeiter: std::slice::Iter<'a, IDLRange>,
    currange: Option<&'a IDLRange>,
    curbit: u64,
}

impl<'a>Iterator for IDLBitRangeIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        while (self.currange.is_some()) {
            let range = self.currange.unwrap();
            while (self.curbit < 64) {
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
        write!(f, "IDLBitRange (compressed) {:?} (decompressed) [ ", self.list);
        for id in self {
            write!(f, "{}, ", id);
        }
        write!(f, "]")
    }
}



#[cfg(test)]
mod tests {
    use super::{IDL, IDLSimple, IDLBitRange};
    use std::fmt::Debug;
    use std::ops::{BitAnd};

    fn test_append<T: IDL + Debug>(mut idl: T) -> bool {
        let x: u64 = 1;
        idl.push_id(x);
        println!("{:?}", idl);
        true
    }

    #[test]
    fn test_simple_append() {
        let mut idl = IDLSimple::new();
        assert!(test_append(idl));
    }

    fn test_intersection_1<T: IDL>(idl_a: &mut T, idl_b: &mut T) {
        idl_a.push_id(1);
        idl_a.push_id(2);
        idl_a.push_id(3);

        idl_b.push_id(2);
    }

    fn test_intersection_2<T: IDL>(idl_a: &mut T, idl_b: &mut T) {
        idl_a.push_id(1);
        idl_a.push_id(2);
        idl_a.push_id(3);

        idl_b.push_id(4);
    }

    fn test_intersection_3<T: IDL>(idl_a: &mut T, idl_b: &mut T) {
        let a: [u64; 9] = [1, 2, 3, 4, 35, 64, 65, 128, 150];
        let b: [u64; 10] = [2, 3, 8, 35, 64, 128, 130, 150, 152, 180];
        for i in a.into_iter() {
            idl_a.push_id(i.clone());
        }
        for i in b.into_iter() {
            idl_b.push_id(i.clone());
        }
    }

    fn test_intersection_4<T: IDL>(idl_a: &mut T, idl_b: &mut T) {
        let b: [u64; 13] = [2, 3, 8, 35, 64, 128, 130, 150, 152, 180, 256, 800, 900];
        for i in 1..1024 {
            idl_a.push_id(i.clone());
        }
        for i in b.into_iter() {
            idl_b.push_id(i.clone());
        }
    }

    #[test]
    fn test_simple_intersection_1() {
        let mut idl_a = IDLSimple::new();
        let mut idl_b = IDLSimple::new();
        test_intersection_1(&mut idl_a, &mut idl_b);

        let idl_result = idl_a & idl_b;
        println!("{:?}", idl_result);
    }

    #[test]
    fn test_range_intersection_1() {
        let mut idl_a = IDLBitRange::new();
        let mut idl_b = IDLBitRange::new();
        test_intersection_1(&mut idl_a, &mut idl_b);

        let idl_result = idl_a & idl_b;
        println!("{:?}", idl_result);
    }

    #[test]
    fn test_simple_intersection_2() {
        let mut idl_a = IDLSimple::new();
        let mut idl_b = IDLSimple::new();
        test_intersection_2(&mut idl_a, &mut idl_b);

        let idl_result = idl_a & idl_b;
        println!("{:?}", idl_result);
    }

    #[test]
    fn test_range_intersection_2() {
        let mut idl_a = IDLBitRange::new();
        let mut idl_b = IDLBitRange::new();
        test_intersection_2(&mut idl_a, &mut idl_b);

        let idl_result = idl_a & idl_b;
        println!("{:?}", idl_result);
    }

    #[test]
    fn test_range_intersection_3() {
        let mut idl_a = IDLBitRange::new();
        let mut idl_b = IDLBitRange::new();
        test_intersection_3(&mut idl_a, &mut idl_b);

        let idl_result = idl_a & idl_b;
        println!("{:?}", idl_result);
    }

    #[test]
    fn test_range_intersection_4() {
        let mut idl_a = IDLBitRange::new();
        let mut idl_b = IDLBitRange::new();
        test_intersection_4(&mut idl_a, &mut idl_b);

        println!("{:?}", idl_a);

        let idl_result = idl_a & idl_b;
        println!("{:?}", idl_result);
    }
}
