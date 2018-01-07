
struct Bst<T> {
    // Contains the root?
    root: Option<BstNode<T>>,
    // Contains garbage lists?
}

struct BstNode<T> {
    data: T,
    left: Option<BstNode<T>>,
    right: Option<BstNode<T>>,
}

impl<T> Bst<T> {
    pub fn new() -> Self {
    }
}



