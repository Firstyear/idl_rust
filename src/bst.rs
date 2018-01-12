
struct Bst<K, V> {
    // Contains the root?
    root: Option<Box<BstNode<K, V>>>,
    // Contains garbage lists?
    length: usize,
}

enum BstNode<K, V> {
    Leaf {
        key: K,
        value: V,
    },
    Branch {
        key: K,
        left: Option<Box<BstNode<K, V>>>,
        right: Option<Box<BstNode<K, V>>>,
    }
}


impl<K, V> Bst<K, V> {
    pub fn new() -> Self {
        Bst {
            root: None,
            length: 0,
        }
    }

    /// Purge everything
    pub fn clear(&mut self) {
        // Because this changes root from Some to None, it moves ownership
        // of root to this function, and all it's descendants that are dropped.
        self.root = None;
        self.length = 0;
    }

    /// insert a value
    pub fn insert(&mut self, key: K, value: V) {
        match self.root.as_ref() {
            /* Empty tree, insert a leaf. */
            None => {
                self.root = Some(Box::new(
                    BstNode::Leaf { key, value }
                ));
                self.length += 1;
            }
            Some(_) => {
            }
        };
    }

    /// Do we contain a key?
    pub fn contains_key(&self, key: &K) {
    }

    /// Delete the value
    pub fn remove(&mut self, key: &K) -> Option<V> {
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

        bst.clear();

        assert!(bst.len() == 0);
    }
}
