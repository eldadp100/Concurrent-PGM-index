#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <cassert>
#include <iterator>
#include <limits>
#include <memory>
#include <new>
#include <set>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>
#include <mutex>
//#include "../oneapi/tbb/concurrent_vector.h"
#include "pgm_concurrent_improved/concurrent_containers/skip_list.hpp"
#include "pgm_concurrent_improved/concurrent_containers/insert_find_only_BST.hpp"
namespace pgm {

    /* should support:
     * 1. insert
     * 2. find -
     * 3. to_vector - returns a sorted vector of the elements
     * */


    template <typename K, typename V, typename Item>
    class GlobalLockVectorBuffer {
    private:
        std::vector<Item> data;
        std::mutex global_lock;
    public:

        GlobalLockVectorBuffer() {

        }

        ~GlobalLockVectorBuffer() {
            delete data;
        }

        void insert(K key, V value) {
            global_lock.lock();
            data.push_back(Item(key, value));
            global_lock.unlock();
        }

        std::pair<K,V> *find(K key) {
            global_lock.lock();
            for (auto i = data.end(); i!= data.begin(); i--) {
                if(i->first == key) {
                    global_lock.unlock();
                    return i->deleted() ? NULL : new std::pair<K,V>(i->first, i->second);
                }
            }
            global_lock.unlock();
            return NULL;
        }

        std::vector<Item> *to_vector() {
            global_lock.lock();
            std::sort(data.begin(), data.end());
            global_lock.unlock();
            return &data;
        }
    };



    template <typename K, typename V, typename Item>
    class GlobalSortedLockVectorBuffer {
    private:
        std::vector<Item> data;
        std::mutex global_lock;
    public:

        GlobalSortedLockVectorBuffer() {

        }

        ~GlobalSortedLockVectorBuffer() {
            delete data;
        }

        void insert(K key, V value) {
            Item new_item = *(new Item(key, value));
            global_lock.lock();
            if (data.size() != 0) {

                auto first = data.begin();
                auto last = data.end();
                auto n = std::distance(first, last);
                while (n > 1) {
                    auto half = n / 2;
                    __builtin_prefetch(&*(first + half / 2), 0, 0);
                    __builtin_prefetch(&*(first + half + half / 2), 0, 0);
                    first = first[half] < key ? first + half : first;
                    n -= half;
                }
                auto insertion_point = first + (*first < key);
                if (insertion_point != data.end() && (*insertion_point).first == new_item.first) {
                    *insertion_point = new_item; // update
                    global_lock.unlock();
                    return;
                }

                // insert to buffer
                data.insert(insertion_point, new_item);
                global_lock.unlock();
            }
            else {
                data.push_back(new_item);
                global_lock.unlock();
            }

        }

        std::pair<K,V> *find(K key) {
            global_lock.lock();
            if (data.size() == 0)
                return NULL;
            auto first = data.begin();
            auto last = data.end();
            auto n = std::distance(first, last);
            while (n > 1) {
                auto half = n / 2;
                __builtin_prefetch(&*(first + half / 2), 0, 0);
                __builtin_prefetch(&*(first + half + half / 2), 0, 0);
                first = first[half] < key ? first + half : first;
                n -= half;
            }
            auto a = first + (*first < key);
            global_lock.unlock();
            return new std::pair<K,V>(a->first, a->second);
        }

        std::vector<Item> *to_vector() {
            return &data;
        }
    };



    template <typename K, typename V, typename Item>
    class ConcurrentSkipListBuffer {
    private:
        SkipList<K,V,Item> data;
    public:

        ConcurrentSkipListBuffer() {

        }

        ~ConcurrentSkipListBuffer() {
        }

        void insert(K key, V value) {
            data.insert(key, value);
        }

        std::pair<K,V> *find(K key) {
            auto a = data.find_wait_free(key);
            return a == NULL ? NULL : new std::pair<K,V> (key, *a);
        }

        std::vector<Item> *to_vector() {
            return data.to_vector();
        }
    };



    template <typename K, typename V, typename Item>
    class ConcurrentBSTBuffer {
    private:
        bst::BST<K,V,Item> data;
    public:

        ConcurrentBSTBuffer() {

        }

        ~ConcurrentBSTBuffer() {
        }

        void insert(K key, V value) {
            data.insert(key, value);
        }

        std::pair<K,V> *find(K key) {
            return data.find(key);
        }

        std::vector<Item> *to_vector() {
            return data.to_vector();
        }
    };




}