
use crossbeam_epoch::{Atomic, Owned, Shared};
use std::ptr;
use std::sync::{Mutex, MutexGuard};

const CAPACITY: usize = 5;
const L_CAPACITY: usize = CAPACITY + 1;

struct Bst<K, V> {
    write: Mutex<()>,
    // Shared root txn?
    active: Atomic<BstTxn<K, V>>,
}

// Diff between write and read txn?

// Does bsttxn impl copy?
struct BstTxn<K, V> {
    u64: tid,
    root: *mut BstNode<K, V>,
    length: usize,
    // Contains garbage lists?
    // How can we set the garbage list of the former that we
    // copy from? Unsafe mut on the active? Mutex on the garbage list?
    // Cell of sometype?
    owned: Vec<*mut BstNode<K, V>>,
}

struct BstLeaf<K, V> {
    /* These options get null pointer optimised for us :D */
    key: [Option<K>; CAPACITY],
    value: [Option<V>; CAPACITY],
    parent: *mut BstNode<K, V>,
    parent_idx: u16,
    capacity: u16,
}

struct BstBranch<K, V> {
    key: [Option<K>; CAPACITY],
    links: [*mut BstNode<K, V>; L_CAPACITY],
    parent: *mut BstNode<K, V>,
    parent_idx: u16,
    capacity: u16,
}

// Do I even need an error type?
enum BstErr {
    Unknown,
}

enum BstNode<K, V> {
    Leaf {
        inner: BstLeaf<K, V>
    },
    Branch {
        inner: BstBranch<K, V>
    }
}

impl <K, V> BstNode<K, V> where
    K: Clone + PartialEq,
    V: Clone,
{
    pub fn new_leaf() -> Self {
        BstNode::Leaf {
            inner: BstLeaf {
                key: [None, None, None, None, None],
                value: [None, None, None, None, None],
                parent:  ptr::null_mut(),
                parent_idx: 0,
                capacity: 1,
            }
        }
    }

    fn new_branch(key: K, left: *mut BstNode<K, V>, right: *mut BstNode<K, V>) -> Self {
        BstNode::Branch {
            inner: BstBranch {
                key: [Some(key), None, None, None, None],
                links: [left, right, ptr::null_mut(), ptr::null_mut(), ptr::null_mut(), ptr::null_mut()],
                parent: ptr::null_mut(),
                parent_idx: 0,
                capacity: 1,
            }
        }
    }

    // Recurse and search.
    pub fn search(&self, key: &K) -> Option<&V> {
        match self {
            &BstNode::Leaf { ref inner } => {
                None
            }
            &BstNode::Branch { ref inner } => {
                None
            }
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<*mut BstNode<K, V>, BstErr> {
        /* Should we auto split? */
        Ok(ptr::null_mut())
    }

    pub fn update(&mut self, key: K, value: V) {
        /* If not present, insert */
        /* If present, replace */
    }

    // Should this be a reference?
    pub fn remove(&mut self, key: &K) -> Option<(K, V)> {
        /* If present, remove */
        /* Else nothing, no-op */
        None
    }

    /* Return if the node is valid */
    fn verify() -> bool {
        false
    }
}

impl<K, V> Bst<K, V> where
    K: Clone + PartialEq,
    V: Clone,
{
    pub fn new() -> Self {
        let new_root = Box::new(
            BstNode::new_leaf()
        );
        // Create the root txn as empty tree.
        Bst {
            // root: None,
            root: Box::into_raw(new_root),
            length: 0,
        }
    }

    /// Purge everything
    /// YOU NEED MAP NODES FOR THIS!!!!
    pub fn clear(&mut self) {
        // Because this changes root from Some to None, it moves ownership
        // of root to this function, and all it's descendants that are dropped.

        /* !!!!!!!!!!!!!!!!!! TAKE OWNERSHIP OF ROOT AND FREE IT !!!!!!!!!!!!!!!!! */

        // With EBR you need to walk the tree and mark everything to be dropped.
        // Perhaps just the root needs EBR marking?
        let new_root = Box::new(
            BstNode::new_leaf()
        );
        self.root = Box::into_raw(new_root);
        self.length = 0;
    }

    pub fn search(&self, key: &K) -> Option<&V> {
        None
    }

    /// insert a value
    pub fn insert(&mut self, key: K, value: V) -> Result<(), BstErr> {
        /* Recursively insert. */
        /* This is probably an unsafe .... */
        (*self.root).insert(key, value).and_then(|nr: *mut _ | {
            self.length += 1;
            self.root = nr;
            Ok(())
        })
        /* IN THE FUTURE you will need to update the root here ... maybe */
    }

    /// Do we contain a key?
    pub fn contains_key(&self, key: &K) {
    }

    /// Delete the value
    pub fn remove(&mut self, key: &K) -> Option<(K, V)> {
        None
    }

    pub fn len(&self) -> usize {
        self.length
    }
}


#[cfg(test)]
mod tests {
    use super::Bst;

    #[test]
    fn test_simple_search() {
        let mut bst: Bst<i64, i64> = Bst::new();
        assert!(bst.len() == 0);

        bst.insert(0, 0);
        bst.insert(1, 1);

        assert!(bst.len() == 2);

        assert!(bst.search(&0) == Some(&0));
        assert!(bst.search(&1) == Some(&1));

        bst.clear();

        assert!(bst.len() == 0);

    }
}
