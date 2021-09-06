/* Test 3 - measures find of random elements.
 *          For more details look at the report (Scalability Tests) */
#include <vector>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include "../include/pgm/pgm_index_dynamic.hpp"
#include <thread>
#include <chrono>

void foo(pgm::DynamicPGMIndex<uint32_t, uint32_t>* dynamic_pgm, std::vector<std::pair<uint32_t, uint32_t>>* data, int tid) {
    for (auto i: *data) {
        dynamic_pgm->find(i.first, tid);
    }
}


int main(int argc, char** argv) {
    int n_threads = atoi(argv[1]);
    int load = atoi(argv[2]);

    pgm::DynamicPGMIndex<uint32_t, uint32_t> dynamic_pgm(8);
    int thread_load = load / n_threads;
    // generate data for threads
    std::vector<std::pair<uint32_t, uint32_t>> data(load);
    std::generate(data.begin(), data.end(), [] { return std::make_pair(std::rand(), 1); });

    for (auto i: data) {
        dynamic_pgm.insert_or_assign(i.first, i.second, 0);
    }

    std::vector<std::pair<uint32_t, uint32_t>>* threads_data[n_threads];
    for (int i = 0; i < n_threads; ++i) {
        threads_data[i] = new std::vector<std::pair<uint32_t, uint32_t>>(thread_load);
        std::generate(threads_data[i]->begin(), threads_data[i]->end(), [] { return std::make_pair(std::rand(), 1); });
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    std::hash<std::thread::id> hasher;
    std::thread t[n_threads];
    for (int i = 0; i < n_threads; ++i) {
        t[i] = std::thread(foo, &dynamic_pgm, threads_data[i], hasher(t->get_id()));
    }

    for (int i = 0; i < n_threads; ++i) {
        t[i].join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto time = end_time - start_time;
    std::cout << "Elapsed " << time/std::chrono::milliseconds(1) << std::endl;

    return 0;
}