
use std::sync::{Mutex, MutexGuard, RwLock, Arc};
use std::ops::Deref;

#[derive(Debug)]
struct CowCellInner<T> {
    /*
     * Later, this needs pointers to the next txn for data ordering.
     */
    pub data: Arc<T>,
    // MAKE Arc<CowCellInner< ...
    // next_txn: Option<Arc
}

impl<T> CowCellInner<T> {
    pub fn new(data: T) -> Self {
        CowCellInner {
            data: Arc::new(data)
        }
    }
}

#[derive(Debug)]
pub struct CowCell<T> {
    write: Mutex<()>,
    active: RwLock<CowCellInner<T>>,
}

impl<T> CowCell<T>
    where T: Clone
{
    pub fn new(data: T) -> Self {
        CowCell {
            write: Mutex::new(()),
            active: RwLock::new(
                CowCellInner::new(data)
            ),
        }
    }

    pub fn begin_read_txn(&self) -> CowCellReadTxn<T> {
        let rwguard = self.active.read().unwrap();
        CowCellReadTxn {
            data: rwguard.data.clone()
        }
    }

    pub fn begin_write_txn(&self) -> CowCellWriteTxn<T> {
        /* Take the exclusive write lock first */
        let mguard = self.write.lock().unwrap();
        /* Now take a ro-txn to get the data copied */
        let rwguard = self.active.read().unwrap();
        let data: T = (*rwguard.data).clone();
        /* Now build the write struct */
        CowCellWriteTxn {
            /* This copies the data */
            work: data,
            caller: self,
            guard: mguard,
        }
    }

    fn commit(&self, newdata: T) {
        let mut rwguard = self.active.write().unwrap();
        *rwguard = CowCellInner::new(newdata);
    }
}

#[derive(Debug)]
pub struct CowCellReadTxn<T> {
    // Just store a pointer to our type, the inner maintains internal
    // ordering refs.
    // inner: CowCellInner<T>,
    data: Arc<T>,
}

impl<T> Deref for CowCellReadTxn<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.data
    }
}

#[derive(Debug)]
pub struct CowCellWriteTxn<'a, T: 'a> {
    // Hold open the guard, and initiate the copy to here.
    work: T,
    // This way we know who to contact for updating our data ....
    caller: &'a CowCell<T>,
    guard: MutexGuard<'a, ()>
}

impl<'a, T> CowCellWriteTxn<'a, T>
    where T: Clone
{
    /* commit */
    /* get_mut data */
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.work
    }

    pub fn commit(self) {
        /* Write our data back to the CowCell */
        self.caller.commit(self.work);
    }
}


#[cfg(test)]
mod tests {
    extern crate time;

    use std::sync::atomic::{AtomicUsize, Ordering};
    use super::CowCell;
    use crossbeam_utils::scoped;

    #[test]
    fn test_simple_create() {
        let data: i64 = 0;
        let cc = CowCell::new(data);

        let cc_rotxn_a = cc.begin_read_txn();
        assert_eq!(*cc_rotxn_a, 0);

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
            }
            assert_eq!(*cc_rotxn_a, 0);

            let cc_rotxn_b = cc.begin_read_txn();
            assert_eq!(*cc_rotxn_b, 0);
            /* The write txn and it's lock is dropped here */
            cc_wrtxn.commit();
        }

        /* Start a new txn and see it's still good */
        let cc_rotxn_c = cc.begin_read_txn();
        assert_eq!(*cc_rotxn_c, 1);
        assert_eq!(*cc_rotxn_a, 0);
    }

    fn mt_writer(cc: &CowCell<i64>) {
        let mut last_value: i64 = 0;
        while last_value < 10 {
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

    fn rt_writer(cc: &CowCell<i64>) {
        let mut last_value: i64 = 0;
        while last_value < 10 {
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
        // Create the new cowcell.
        let data: i64 = 0;
        let cc = CowCell::new(data);

        scoped::scope(|scope| {
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
        print!("Arc MT create :{} ", end - start);
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

    fn test_gc_operation_thread(cc: &CowCell<TestGcWrapper<i64>>) {
        while GC_COUNT.load(Ordering::Acquire) < 50 {
            // thread::sleep(std::time::Duration::from_millis(200));
            {
                let mut cc_wrtxn = cc.begin_write_txn();
                {
                    let mut_ptr = cc_wrtxn.get_mut();
                    mut_ptr.data = mut_ptr.data + 1;
                }
                cc_wrtxn.commit();
            }
        }
    }

    #[test]
    fn test_gc_operation() {
        let data = TestGcWrapper{data: 0};
        let cc = CowCell::new(data);

        scoped::scope(|scope| {
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


