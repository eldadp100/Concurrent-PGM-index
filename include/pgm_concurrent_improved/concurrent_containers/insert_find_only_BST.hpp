#include <mutex>
#include <atomic>

namespace bst {

    template <typename K, typename V, typename Item>
    class BST {
    private:
        class Node {
        public:
            std::mutex node_lock;
            K key;
            V value;
            std::atomic<Node*> left = NULL;
            std::atomic<Node*> right = NULL;

            Node(K _key, V& _value) {
                key = _key;
                value = _value;
            }

            Node() { }

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
            Node *curr = root->right.load();
            while (true) {
                while (curr != NULL) {
                    if (k == curr->key) {
                        curr->value = v;
                        return;
                    } else if (k > curr->key) {
                        prev = curr;
                        curr = curr -> right.load();
                    } else {
                        prev = curr;
                        curr = curr -> left.load();
                    }
                }
                prev->node_lock.lock();
                if ((k>prev->key && prev->right == NULL) || (k<prev->key && prev->left == NULL)){
                    if (k>prev->key) {
                        prev->right.store(new_node) ;
                    } else {
                        prev->left.store(new_node);
                    }
                    prev->node_lock.unlock();
                    return;
                }
                prev->node_lock.unlock();
                curr = prev;
            }
        }

        Item *find(K k) {
            Node *curr = root->right;
            while (curr != NULL) {
                if (k == curr->key) {
                    return &curr->value;
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
            ret->push_back(the_root->value);
            to_vector_rec(the_root->right, ret);
        }
    };
}
