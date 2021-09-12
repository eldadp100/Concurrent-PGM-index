


/* Unit Test 1 - concurrent inserts then check that is found.
 *          For more details look at the report (Unit Tests) */
#include <vector>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include "../include/pgm_concurrent_improved/concurrent_containers/insert_find_only_BST.hpp"
#include <thread>
#include <chrono>

int n_threads = 8;
int load = 1000000;

void foo(bst::BST<uint32_t, uint32_t, uint32_t>* dynamic_pgm, std::vector<std::pair<uint32_t, uint32_t>>* data, int tid) {
    for (auto i: *data) {
        dynamic_pgm->insert(i.first, i.second);
    }
}


int main() {
    bst::BST<uint32_t, uint32_t, uint32_t> _bst;
    int thread_load = load / n_threads;
    // generate data for threads
    std::vector<std::pair<uint32_t, uint32_t>>* threads_data[n_threads];
    for (int i = 0; i < n_threads; ++i) {
        threads_data[i] = new std::vector<std::pair<uint32_t, uint32_t>>(thread_load);
        std::generate(threads_data[i]->begin(), threads_data[i]->end(), [] { return std::make_pair(std::rand(), 1); });
    }

    std::hash<std::thread::id> hasher;
    std::thread t[n_threads];
    for (int i = 0; i < n_threads; ++i) {
        t[i] = std::thread(foo, &_bst, threads_data[i], hasher(t->get_id()));
    }

    for (int i = 0; i < n_threads; ++i) {
        t[i].join();
    }

    bool ret = true;
    uint32_t r;
    for (int i = 0; i < n_threads; ++i) {
        for (auto item: *threads_data[i]) {
            if (_bst.find(item.first) == NULL) {
                std::cout << "FAIL on " << item.first << " Inserted but not found\n";
                ret = false;
            }
        }
    }

    if (ret) {
        std::cout << "PASS \n";
    }

    return 0;
}