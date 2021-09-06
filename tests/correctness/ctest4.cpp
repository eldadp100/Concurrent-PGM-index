/* Unit Test 4 - concurrent inserts then deletes and check that empty.
 *          For more details look at the report (Unit Tests) */
#include <vector>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include "../include/pgm/pgm_index_dynamic.hpp"
#include <thread>
#include <chrono>

int n_threads = 4;
int load = 1000000;

void foo(pgm::DynamicPGMIndex<uint32_t, uint32_t>* dynamic_pgm, std::vector<std::pair<uint32_t, uint32_t>>* data, int tid) {
    for (auto i: *data) {
        dynamic_pgm->erase(i.first, tid);
    }
}


int main() {
    pgm::DynamicPGMIndex<uint32_t, uint32_t> dynamic_pgm(8);
    int thread_load = load / n_threads;
    // generate data for threads
    std::vector<std::pair<uint32_t, uint32_t>>* threads_data[n_threads];
    for (int i = 0; i < n_threads; ++i) {
        threads_data[i] = new std::vector<std::pair<uint32_t, uint32_t>>(thread_load);
        std::generate(threads_data[i]->begin(), threads_data[i]->end(), [] { return std::make_pair(std::rand(), 1); });
        for (auto item: *threads_data[i]) {
            dynamic_pgm.insert_or_assign(item.first, item.second, 0);
        }
    }

    std::hash<std::thread::id> hasher;
    std::thread t[n_threads];
    for (int i = 0; i < n_threads; ++i) {
        t[i] = std::thread(foo, &dynamic_pgm, threads_data[i], hasher(t->get_id()));
    }

    for (int i = 0; i < n_threads; ++i) {
        t[i].join();
    }

    bool ret = true;
    for (int i = 0; i < n_threads; ++i) {
        for (auto item: *threads_data[i]) {
            auto a = dynamic_pgm.find(item.first, 0);
            if (a != NULL && a->first == item.first) {
                std::cout << "FAIL on " << item.first << " Deleted but found \n";
                ret = false;
            } else {
//                std::cout << "SUCCESS on " << item.first << " \n";
            }
        }
    }

    if (ret) {
        std::cout << "PASS \n";
    }

    return 0;
}