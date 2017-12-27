
use crossbeam::epoch::{self, Atomic, Owned, Guard};
use std::sync::atomic::Ordering::{Relaxed, Release};

use std::sync::{Mutex, MutexGuard};
use std::ops::Deref;

#[derive(Debug)]
pub struct EbrCell<T> {
    write: Mutex<()>,
    active: Atomic<T>,
}

impl<T> EbrCell<T>
    where T: Clone
{
    pub fn new(data: T) -> Self {
        EbrCell {
            write: Mutex::new(()),
            active: Atomic::new(data)
        }
    }

    pub fn begin_write_txn(&self) -> EbrCellWriteTxn<T> {
        /* Take the exclusive write lock first */
        let mguard = self.write.lock().unwrap();
        /* Do an atomic load of the current value */
        let guard = epoch::pin();
        let cur_shared = self.active.load(Relaxed, &guard).unwrap();
        /* This is the 'copy' of the copy on write! */
        let data: T = (*cur_shared).clone();
        /* Now build the write struct, we'll discard the pin shortly! */
        EbrCellWriteTxn {
            work: data,
            caller: self,
            guard: mguard,
        }
    }

    fn commit(&self, newdata: T) {
        // Yield a read txn?
        let guard = epoch::pin();

        // Load the previous data ready for unlinking
        let _prev_data = self.active.load(Relaxed, &guard).unwrap();
        // Make the data Owned, and set it in the active.
        let owned_data: Owned<T> = Owned::new(newdata);
        let _shared_data = self.active.store_and_ref(owned_data, Release, &guard);
        // Finally, set our previous data for cleanup.
        unsafe {
            guard.unlinked(_prev_data);
        }
        // Then return the current data with a readtxn. Do we need a new guard scope?
    }

    pub fn begin_read_txn(&self) -> EbrCellReadTxn<T> {
        let guard = epoch::pin();

        // This option returns None on null pointer, but we can never be null
        // as we have to init with data, and all replacement ALWAYS gives us
        // a ptr, so unwrap?
        let cur = {
            let c = self.active.load(Relaxed, &guard).unwrap();
            c.as_raw()
        };

        EbrCellReadTxn {
            guard: guard,
            data: cur,
        }
    }
}

impl<T> Drop for EbrCell<T> {
    fn drop(&mut self) {
        // Right, we are dropping! Everything is okay here *except*
        // that we need to tell our active data to be unlinked, else it may
        // be dropped "unsafely".
        let guard = epoch::pin();

        let _prev_data = self.active.load(Relaxed, &guard).unwrap();
        unsafe {
            guard.unlinked(_prev_data);
        }
    }
}

#[derive(Debug)]
pub struct EbrCellReadTxn<T> {
    guard: Guard,
    data: *const T,
}

impl<T> Deref for EbrCellReadTxn<T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe {
            &(*self.data)
        }
    }
}

#[derive(Debug)]
pub struct EbrCellWriteTxn<'a, T: 'a> {
    // Hold open the guard, and initiate the copy to here.
    work: T,
    // This way we know who to contact for updating our data ....
    caller: &'a EbrCell<T>,
    guard: MutexGuard<'a, ()>
}

impl<'a, T> EbrCellWriteTxn<'a, T>
    where T: Clone
{
    /* commit */
    /* get_mut data */
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.work
    }

    pub fn commit(&self) {
        /* Write our data back to the EbrCell */
        // This is not 100% efficent as possible, work out a better solution
        // later that avoids .clone()
        self.caller.commit(self.work.clone());
    }
}


#[cfg(test)]
mod tests {
    extern crate crossbeam;
    extern crate time;
    use std::sync::atomic::{AtomicUsize, Ordering};


    use super::EbrCell;

    #[test]
    fn test_simple_create() {
        let data: i64 = 0;
        let cc = EbrCell::new(data);

        let cc_rotxn_a = cc.begin_read_txn();
        assert_eq!(*cc_rotxn_a, 0);
        println!("rotxn_a {}", *cc_rotxn_a);

        {
            /* Take a write txn */
            let mut cc_wrtxn = cc.begin_write_txn();
            /* Get the data ... */
            {
                let mut_ptr = cc_wrtxn.get_mut();
                /* Assert it's 0 */
                assert_eq!(*mut_ptr, 0);
                *mut_ptr = 1;
                assert_eq!(*mut_ptr, 1);
                println!("wrtxn {}", *mut_ptr);
            }
            assert_eq!(*cc_rotxn_a, 0);
            println!("rotxn_a {}", *cc_rotxn_a);

            let cc_rotxn_b = cc.begin_read_txn();
            assert_eq!(*cc_rotxn_b, 0);
            println!("rotxn_b {}", *cc_rotxn_b);
            /* The write txn and it's lock is dropped here */
            cc_wrtxn.commit();
        }

        /* Start a new txn and see it's still good */
        let cc_rotxn_c = cc.begin_read_txn();
        assert_eq!(*cc_rotxn_c, 1);
        println!("rotxn_c {}", *cc_rotxn_c);
        assert_eq!(*cc_rotxn_a, 0);
        println!("rotxn_a {}", *cc_rotxn_a);
    }

    fn mt_writer(cc: &EbrCell<i64>) {
        let mut last_value: i64 = 0;
        while last_value < 500 {
            let mut cc_wrtxn = cc.begin_write_txn();
            {
                let mut_ptr = cc_wrtxn.get_mut();
                assert!(*mut_ptr >= last_value);
                last_value = *mut_ptr;
                *mut_ptr = *mut_ptr + 1;
            }
            cc_wrtxn.commit();
        }
    }

    fn rt_writer(cc: &EbrCell<i64>) {
        let mut last_value: i64 = 0;
        while last_value < 500 {
            let cc_rotxn = cc.begin_read_txn();
            {
                assert!(*cc_rotxn >= last_value);
                last_value = *cc_rotxn;
            }
        }
    }

    #[test]
    fn test_multithread_create() {

        let start = time::now();
        // Create the new ebrcell.
        let data: i64 = 0;
        let cc = EbrCell::new(data);

        crossbeam::scope(|scope| {
            let cc_ref = &cc;

            let _readers: Vec<_> = (0..7).map(|_| {
                scope.spawn(move || {
                    rt_writer(cc_ref);
                })
            }).collect();

            let _writers: Vec<_> = (0..3).map(|_| {
                scope.spawn(move || {
                    mt_writer(cc_ref);
                })
            }).collect();

        });

        let end = time::now();
        println!("Ebr MT create :{}", end - start);
    }

    static GC_COUNT: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug, Clone)]
    struct TestGcWrapper<T> {
        data: T
    }

    impl<T> Drop for TestGcWrapper<T> {
        fn drop(&mut self) {
            // Add to the atomic counter ...
            GC_COUNT.fetch_add(1, Ordering::Release);
        }
    }

    fn test_gc_operation_thread(cc: &EbrCell<TestGcWrapper<i64>>) {
        while GC_COUNT.load(Ordering::Acquire) < 50 {
            let mut cc_wrtxn = cc.begin_write_txn();
            {
                let mut_ptr = cc_wrtxn.get_mut();
                mut_ptr.data = mut_ptr.data + 1;
            }
            cc_wrtxn.commit();
        }
    }

    #[test]
    fn test_gc_operation() {
        let data = TestGcWrapper{data: 0};
        let cc = EbrCell::new(data);

        crossbeam::scope(|scope| {
            let cc_ref = &cc;
            let _writers: Vec<_> = (0..3).map(|_| {
                scope.spawn(move || {
                    test_gc_operation_thread(cc_ref);
                })
            }).collect();
        });

        assert!(GC_COUNT.load(Ordering::Acquire) >= 50);
    }

}


