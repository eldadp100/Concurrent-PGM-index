
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

void foo(pgm::DynamicPGMIndex<uint32_t, uint32_t>* dynamic_pgm, std::vector<std::pair<uint32_t, uint32_t>>* range_data, int tid) {
    for (int i=0; i<range_data->size(); ++i) {
        dynamic_pgm->range((*range_data)[i].first,(*range_data)[i].first + (*range_data)[i].second, tid);
    }
}


int main(int argc, char** argv) {
    int n_threads = atoi(argv[1]);
    int load = atoi(argv[2]);

    pgm::DynamicPGMIndex<uint32_t, uint32_t> dynamic_pgm(8);
    int thread_load = load / n_threads;

    // generate data for threads
    std::vector<std::pair<uint32_t, uint32_t>> data_range(load);
    std::generate(data_range.begin(), data_range.end(), [] { return std::make_pair(std::rand() / 2, std::rand() % max_range_size); });
    for (auto i: data_range) {
        for (uint32_t a=i.first; a <= i.first + i.second; ++a) {
            dynamic_pgm.insert_or_assign(a, 1, 0);
        }
    }
    std::vector<std::pair<uint32_t, uint32_t>>* threads_range_data[n_threads];
    for (int i = 0; i < n_threads; ++i) {
        threads_range_data[i] = new std::vector<std::pair<uint32_t, uint32_t>>(data_range.begin() + i * thread_load, data_range.begin() + (i + 1) * thread_load);
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    std::hash<std::thread::id> hasher;
    std::thread t[n_threads];
    for (int i = 0; i < n_threads; ++i) {
        t[i] = std::thread(foo, &dynamic_pgm, threads_range_data[i],  hasher(t->get_id()));
    }

    for (int i = 0; i < n_threads; ++i) {
        t[i].join();
    }


    auto end_time = std::chrono::high_resolution_clock::now();
    auto time = end_time - start_time;
    std::cout << "Elapsed " << time/std::chrono::milliseconds(1) << std::endl;

    return 0;
}