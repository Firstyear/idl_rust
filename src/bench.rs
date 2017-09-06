
extern crate idl_poc;
extern crate time;

use idl_poc::{IDL, IDLSimple, IDLBitRange};
use std::iter::FromIterator;

// Trying to make these work with trait bounds is literally too hard
// So just make our own impls.

fn simple_consume_results(idl: &IDLSimple) -> u64
{
    let mut result: u64 = 0;
    for id in idl {
        result += id;
    }
    return result;
}

fn range_consume_results(idl: &IDLBitRange) -> u64
{
    let mut result: u64 = 0;
    for id in idl {
        result += id;
    }
    return result;
}
fn bench_simple_union(id: &str, a: Vec<u64>, b: Vec<u64>) {
    let idl_a = IDLSimple::from_iter(a);
    let idl_b = IDLSimple::from_iter(b);

    let start = time::now();
    let idl_result = idl_a | idl_b;
    let result = simple_consume_results(&idl_result);
    let end = time::now();
    println!("simple union {}: {} -> {}", id, end - start, result);
}

fn bench_range_union(id: &str, a: Vec<u64>, b: Vec<u64>) {
    let idl_a = IDLBitRange::from_iter(a);
    let idl_b = IDLBitRange::from_iter(b);

    let start = time::now();
    let idl_result = idl_a | idl_b;
    let result = range_consume_results(&idl_result);
    let end = time::now();
    println!("range union  {}: {} -> {}", id, end - start, result);
}


fn bench_simple_intersection(id: &str, a: Vec<u64>, b: Vec<u64>) {
    let idl_a = IDLSimple::from_iter(a);
    let idl_b = IDLSimple::from_iter(b);

    let start = time::now();
    let idl_result = idl_a & idl_b;
    let result = simple_consume_results(&idl_result);
    let end = time::now();
    println!("simple intersect {}: {} -> {}", id, end - start, result);
}

fn bench_range_intersection(id: &str, a: Vec<u64>, b: Vec<u64>) {
    let idl_a = IDLBitRange::from_iter(a);
    let idl_b = IDLBitRange::from_iter(b);

    let start = time::now();
    let idl_result = idl_a & idl_b;
    let result = range_consume_results(&idl_result);
    let end = time::now();
    println!("range intersect  {}: {} -> {}", id, end - start, result);
}

fn test_duplex(id: &str, a: Vec<u64>, b: Vec<u64>) {
    bench_simple_intersection(id, a.clone(), b.clone());
    bench_range_intersection(id, a.clone(), b.clone());
    bench_simple_union(id, a.clone(), b.clone());
    bench_range_union(id, a.clone(), b.clone());
    println!("=====");
}

fn main() {
    test_duplex(
        "1",
        vec![2, 3, 8, 35, 64, 128, 130, 150, 152, 180, 256, 800, 900],
        Vec::from_iter(1..1024)
    );
    test_duplex(
        "2",
        vec![1],
        Vec::from_iter(1..102400)
    );
    test_duplex(
        "3",
        vec![102399],
        Vec::from_iter(1..102400)
    );
    test_duplex(
        "4",
        Vec::from_iter(1..1024),
        Vec::from_iter(1..1024)
    );
    test_duplex(
        "5",
        Vec::from_iter(1..102400),
        Vec::from_iter(1..102400)
    );
    test_duplex(
        "6",
        vec![1],
        vec![1],
    );
    test_duplex(
        "7",
        vec![1],
        vec![2],
    );
    test_duplex(
        "8",
        vec![16],
        Vec::from_iter(1..32)
    );
    let mut vec1 = Vec::new();
    for i in 1..300 {
        vec1.push(64*i + 5)
    }
    let mut vec2 = Vec::new();
    for i in 200..500 {
        vec2.push(64*i + 5)
    }
    test_duplex(
        "9",
        vec1,
        vec2
    );
    let mut vec1 = Vec::new();
    for i in 1..300 {
        vec1.push(64*i + 5);
        vec1.push(64*i + 7);
        vec1.push(64*i + 15)
    }
    let mut vec2 = Vec::new();
    for i in 200..500 {
        vec2.push(64*i + 5);
        vec2.push(64*i + 7);
        vec2.push(64*i + 15)
    }
    test_duplex(
        "10",
        vec1,
        vec2
    );
}

