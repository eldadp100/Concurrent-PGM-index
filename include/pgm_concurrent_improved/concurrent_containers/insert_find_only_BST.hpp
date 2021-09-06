#include <mutex>


namespace bst {

    template <typename K, typename V, typename Item>
    class BST {
    private:
        class Node {
        public:
            std::mutex node_lock;
            K key;
            V value;
            Node *left = NULL;
            Node *right = NULL;
            bool is_root;

            Node(K _key, V _value) {
                key = _key;
                value = _value;
            }

            Node() {
                is_root = true;
            }

            bool unsafe_insert_right(Node* child) {
                if (right == NULL) {
                    right = child;
                    return true;
                }
                return false;
            }

            bool unsafe_insert_left(Node* child) {
                if (left == NULL) {
                    left = child;
                    return true;
                }
                return false;
            }

            // pre condition - value = v
            bool safe_update(V v) {
                node_lock.lock();
                if (value != v) {
                    node_lock.unlock();
                    return false;
                }
                value = v;
                node_lock.unlock();
                return true;
            }
        };

        Node *root = new Node();

        void delete_bottom_up(Node *the_root) {
            if (the_root == NULL)
                return;
            delete_bottom_up(the_root->left);
            delete_bottom_up(the_root->right);
            delete the_root;
        }

    public:
        BST() {

        }
        ~BST() {
            delete_bottom_up(root->right);
        }
        void insert(K k, V v) {
            Node *new_node = new Node(k, v);
            Node *prev = root;
            Node *curr = root->right;
            while (true) {
                while (curr != NULL) {
                    if (k == curr->key) {
                        if (curr->safe_update(v)) {
                            return;
                        }
                    } else if (k > curr->key) {
                        prev = curr;
                        curr = curr -> right;
                    } else {
                        prev = curr;
                        curr = curr -> left;
                    }
                }
                prev->node_lock.lock();
                if ((k>prev->key && prev->right == NULL) || (k<prev->key && prev->left == NULL)){
                    if (k>prev->key) {
                        prev->unsafe_insert_right(new_node);
                    } else {
                        prev->unsafe_insert_left((new_node));
                    }
                    prev->node_lock.unlock();
                    return;
                }
                prev->node_lock.unlock();
                curr = prev;
            }
        }

        std::pair<K,V> *find(K k) {
            Node *curr = root->right;
            while (curr != NULL) {
                if (k == curr->key) {
                    return new std::pair<K, V>(k, curr->value);
                } else if (k > curr->key) {
                    curr = curr -> right;
                } else {
                    curr = curr -> left;
                }
            }
            return NULL;
        }

        std::vector<Item> *to_vector() {
            std::vector<Item> *ret = new std::vector<Item>();
            to_vector_rec(root->right, ret);
            return ret;
        }
        void to_vector_rec(Node* the_root, std::vector<Item> *ret) {
            if (the_root == NULL)
                return;
            to_vector_rec(the_root->left, ret);
            ret->push_back(*(new Item(the_root->key,the_root->value)));
            to_vector_rec(the_root->right, ret);
        }
    };
}
