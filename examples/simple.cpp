/*
 * This example shows how to use pgm::DynamicPGMIndex, a std::map-like container supporting inserts and deletes.
 * Compile with:
 *   g++ updates.cpp -std=c++17 -I../include -o updates
 * Run with:
 *   ./updates
 */

#include <vector>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include "pgm/pgm_index_dynamic.hpp"
#include <thread>
#include <chrono>

int n_threads = 4;
int load = 500000;

void foo(pgm::DynamicPGMIndex<uint32_t, uint32_t>* dynamic_pgm, int k) {
    for (int i=1; i<load; i++) {
        if (i % k == 0) {
            dynamic_pgm->insert_or_assign(i, 1);
            std::cout<<"put "<< i << std::endl;
        }
    }
}


int main() {
    auto start_time = std::chrono::high_resolution_clock::now();
    // Generate some random key-value pairs to bulk-load the Dynamic PGM-index
    std::vector<std::pair<uint32_t, uint32_t>> data(load);
    std::generate(data.begin(), data.end(), [] { return std::make_pair(std::rand(), std::rand()); });
    std::sort(data.begin(), data.end());

    // Construct and bulk-load the Dynamic PGM-index
    pgm::DynamicPGMIndex<uint32_t, uint32_t> dynamic_pgm(data.begin(), data.end());

    std::thread t[n_threads];
    for (int i = 0; i < n_threads; ++i) {
        t[i] = std::thread(foo, &dynamic_pgm, i+29);
    }
    std::cout << "Launched from the main\n";
    for (int i = 0; i < n_threads; ++i) {
        t[i].join();
    }


    for (int j=1; j<load; j++) {
        bool b = false;
        for (int i=0; i<n_threads; i++) {
            if (j % (i+29) == 0)
                b = true;
        }

        bool found = !(dynamic_pgm.find(j) == NULL);

        if (b && found) {
            std::cout << "POSITIVE: " << j << " exists and found" << std::endl;
        } else if (b && !found) {
            std::cout << "NEGATIVE: "<< j << " exists and not found" << std::endl;
        } else if (!b && found) {
            std::cout << "NEGATIVE: "<< j << " not exists but found" << std::endl;
        }

    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto time = end_time - start_time;
    std::cout << "Elapsed " << time/std::chrono::milliseconds(1) << std::endl;


    //    // Insert some data
    //    dynamic_pgm.insert_or_assign(2, 4);
    //    dynamic_pgm.insert_or_assign(4, 8);
    //    dynamic_pgm.insert_or_assign(8, 16);
    //
    //    // Delete data
    //    dynamic_pgm.erase(4);
    //
    //    // Query the container
    //    std::cout << "Container size (data + index) = " << dynamic_pgm.size_in_bytes() << " bytes" << std::endl;
    //    std::cout << "find(4) = " << (dynamic_pgm.find(4) == dynamic_pgm.end() ? "not found" : "found") << std::endl;
    //    std::cout << "find(8)->second = " << dynamic_pgm.find(8)->second << std::endl;
    //
    //    std::cout << "Range search [1, 10000) = ";
    //    auto result = dynamic_pgm.range(1, 10000);
    //    for (auto[k, v] : result)
    //        std::cout << "(" << k << "," << v << "), ";

    return 0;
}