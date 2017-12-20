
use crossbeam::epoch::{self, Atomic, Owned, Guard, Shared};
use std::sync::atomic::Ordering::{Acquire, Release};

use std::sync::{Mutex, MutexGuard};
use std::ops::Deref;

#[derive(Debug)]
pub struct EbrCell<T> {
    write: Mutex<()>,
    active: Atomic<T>,
}

impl<T> EbrCell<T>
    where T: Copy
{
    pub fn new(data: T) -> Self {
        EbrCell {
            write: Mutex::new(()),
            active: Atomic::new(data)
        }
    }

    pub fn begin_read_txn<'a>(&self, g_ref: &'a Guard) -> EbrCellReadTxn<'a, T> {
        // When we generate the guard here and give it to the new struct
        // we get a lifetime error, even though we *should* live as long
        // as the result, which is buond by 'a ...

        // let guard = epoch::pin();

        // This option returns None on null pointer, but we can never be null
        // as we have to init with data, and all replacement ALWAYS gives us
        // a ptr, so unwrap?
        // let g_ref = &guard;

        let cur = self.active.load(Acquire, g_ref).unwrap();

        EbrCellReadTxn {
            // guard: guard,
            g_ref: g_ref,
            data: cur,
        }
    }

    pub fn begin_write_txn(&self) -> EbrCellWriteTxn<T> {
        /* Take the exclusive write lock first */
        let mguard = self.write.lock().unwrap();
        /* Do an atomic load of the current value */
        let guard = epoch::pin();
        let cur_shared = self.active.load(Acquire, &guard).unwrap();
        /* This is the 'copy' of the copy on write! */
        let data: T = **cur_shared;
        /* Now build the write struct, we'll discard the pin shortly! */
        /* Should we give this a copy of the atomic pointer? */
        EbrCellWriteTxn {
            work: data,
            caller: self,
            guard: mguard,
        }
    }

    fn commit(&self, newdata: T) {
        // Yield a read txn?
        let guard = epoch::pin();

        // Make the data Owned.
        let owned_data: Owned<T> = Owned::new(newdata);

        let _shared_data = self.active.store_and_ref(owned_data, Release, &guard);
    }
}

#[derive(Debug)]
pub struct EbrCellReadTxn<'a, T: 'a> {
    // guard: Guard,
    g_ref: &'a Guard,
    data: Shared<'a, T>,
}

impl<'a, T> Deref for EbrCellReadTxn<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.data
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
    where T: Copy
{
    /* commit */
    /* get_mut data */
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.work
    }

    pub fn commit(&self) {
        /* Write our data back to the EbrCell */
        self.caller.commit(self.work);
    }
}


#[cfg(test)]
mod tests {
    extern crate crossbeam;
    extern crate time;
    use crossbeam::epoch;

    use super::EbrCell;

    #[test]
    fn test_simple_create() {
        let data: i64 = 0;
        let cc = EbrCell::new(data);

        let guard = epoch::pin();

        let cc_rotxn_a = cc.begin_read_txn(&guard);
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

            let cc_rotxn_b = cc.begin_read_txn(&guard);
            assert_eq!(*cc_rotxn_b, 0);
            println!("rotxn_b {}", *cc_rotxn_b);
            /* The write txn and it's lock is dropped here */
            cc_wrtxn.commit();
        }

        /* Start a new txn and see it's still good */
        let cc_rotxn_c = cc.begin_read_txn(&guard);
        assert_eq!(*cc_rotxn_c, 1);
        println!("rotxn_c {}", *cc_rotxn_c);
        assert_eq!(*cc_rotxn_a, 0);
        println!("rotxn_a {}", *cc_rotxn_a);
    }

    fn mt_writer(cc: &EbrCell<i64>) {
        let mut last_value: i64 = 0;
        while last_value < 100 {
            let mut cc_wrtxn = cc.begin_write_txn();
            {
                let mut_ptr = cc_wrtxn.get_mut();
                last_value = *mut_ptr;
                *mut_ptr = *mut_ptr + 1;
            }
            cc_wrtxn.commit();
        }
    }

    fn rt_writer(cc: &EbrCell<i64>) {
        let mut last_value: i64 = 0;
        while last_value < 100 {
            let guard = epoch::pin();
            let cc_rotxn = cc.begin_read_txn(&guard);
            {
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
}


