/* Test 5 - measures parallel insert and find (of existing elements).
 *          For more details look at the report (Scalability Tests) */
#include <vector>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include "../include/pgm/pgm_index_dynamic.hpp"
#include <thread>
#include <chrono>

int n_threads = 2;
int load = 1000000;
bool b = true;

void foo_insert(pgm::DynamicPGMIndex<uint32_t, uint32_t>* dynamic_pgm, std::vector<std::pair<uint32_t, uint32_t>>* insert_data, int tid) {
    for (int i=0; i<insert_data->size(); ++i) {
        dynamic_pgm->insert_or_assign((*insert_data)[i].first, (*insert_data)[i].second, tid);
    }
}

void foo_find(pgm::DynamicPGMIndex<uint32_t, uint32_t>* dynamic_pgm, std::vector<std::pair<uint32_t, uint32_t>>* find_data, int tid) {
    for (int i=0; i<find_data->size(); ++i) {
        if (dynamic_pgm->find((*find_data)[i].first, tid) == NULL) {
            b = false; // legal in concurrent setting, atomic isn't necessary
            std::cout << "FAIL on "<< (*find_data)[i].first << "\n";
        }
    }
}


int main() {
    pgm::DynamicPGMIndex<uint32_t, uint32_t> dynamic_pgm(8);
    int thread_load = load / n_threads;

    // generate data for threads
    std::vector<std::pair<uint32_t, uint32_t>> data_find(load);
    std::generate(data_find.begin(), data_find.end(), [] { return std::make_pair(std::rand(), 1); });
    for (auto i: data_find) {
        dynamic_pgm.insert_or_assign(i.first, i.second, 0);
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
    std::thread t[n_threads * 2];
    for (int i = 0; i < n_threads; ++i) {
        t[2 * i] = std::thread(foo_find, &dynamic_pgm, threads_find_data[i], hasher(t->get_id()));
        t[2 * i + 1] = std::thread(foo_insert, &dynamic_pgm, threads_insert_data[i], hasher(t->get_id()));
    }

    for (int i = 0; i < n_threads * 2; ++i) {
        t[i].join();
    }

    if (b) {
        std::cout << "PASS \n";
    }
    return 0;
}