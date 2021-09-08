/* Test 5 - measures parallel insert and find (of existing elements).
 *          For more details look at the report (Scalability Tests) */
#include <vector>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include "../include/pgm/pgm_index_dynamic.hpp"
#include <thread>
#include <chrono>

int n_threads = 4;
int load = 50000;
int max_range_size = 256;
bool b = true;

void foo(pgm::DynamicPGMIndex<uint32_t, uint32_t>* dynamic_pgm, std::vector<std::pair<uint32_t, uint32_t>>* range_data, std::vector<std::pair<uint32_t, uint32_t>>* insert_data, int tid) {
    uint32_t r;
    for (int i=0; i<insert_data->size(); ++i) {
        dynamic_pgm->insert_or_assign((*insert_data)[i].first, (*insert_data)[i].second, tid);
        auto range_found = dynamic_pgm->range((*range_data)[i].first,(*range_data)[i].first + (*range_data)[i].second, tid);
        for (uint32_t a=(*range_data)[i].first; a <= (*range_data)[i].first + (*range_data)[i].second; ++a) {
            if (!dynamic_pgm->find(a, r, tid)) {
                b = false; // the range is not full
            }
        }
    }
}


int main() {
    pgm::DynamicPGMIndex<uint32_t, uint32_t> dynamic_pgm(8);
    int thread_load = load / n_threads;

    // generate data for threads
    std::vector<std::pair<uint32_t, uint32_t>> data_find(load);
    std::generate(data_find.begin(), data_find.end(), [] { return std::make_pair(std::rand() / 2, std::rand() % max_range_size); });
    for (auto i: data_find) {
        for (uint32_t a=i.first; a <= i.first + i.second; ++a) {
            dynamic_pgm.insert_or_assign(a, 1, 0);
        }
    }
    std::vector<std::pair<uint32_t, uint32_t>>* threads_find_data[n_threads];
    for (int i = 0; i < n_threads; ++i) {
        threads_find_data[i] = new std::vector<std::pair<uint32_t, uint32_t>>(data_find.begin() + i*thread_load, data_find.begin() + (i+1)*thread_load);
    }

    std::vector<std::pair<uint32_t, uint32_t>> data_insert(load);
    std::generate(data_insert.begin(), data_insert.end(), [] { return std::make_pair(std::rand(), 1); });
    std::vector<std::pair<uint32_t, uint32_t>>* threads_insert_data[n_threads];
    for (int i = 0; i < n_threads; ++i) {
        threads_insert_data[i] = new std::vector<std::pair<uint32_t, uint32_t>>(data_insert.begin() + i*thread_load, data_insert.begin() + (i+1)*thread_load);
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    std::hash<std::thread::id> hasher;
    std::thread t[n_threads];
    for (int i = 0; i < n_threads; ++i) {
        t[i] = std::thread(foo, &dynamic_pgm, threads_find_data[i], threads_insert_data[i], hasher(t->get_id()));
    }

    for (int i = 0; i < n_threads; ++i) {
        t[i].join();
    }

    if (b) {
        std::cout << "PASS \n";
    } else {
        std::cout << "FAIL \n";
    }
    return 0;
}