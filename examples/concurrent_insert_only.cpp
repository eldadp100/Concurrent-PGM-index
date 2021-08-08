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
#include <thread>
#include "pgm/pgm_index_dynamic.hpp"


void do_inserts_and_contains(pgm::DynamicPGMIndex<uint32_t, uint32_t> *dynamic_pgm) {
  
    // Insert some data
    std::vector<std::pair<uint32_t, uint32_t>> insert_data(100);
    std::generate(insert_data.begin(), insert_data.end(), [] { return std::make_pair(std::rand(), std::rand()); });
    for(std::pair<uint32_t, uint32_t> i : insert_data) {
      dynamic_pgm->insert_or_assign(i.first, i.second);  
    }
    
    // Query the container
    for(std::pair<uint32_t, uint32_t> i : insert_data) {
      if (dynamic_pgm->find(i.first) != dynamic_pgm->end()) {
        std::cout << "correct" << std::endl;
      } else {
        std::cout << "wrong" << std::endl;
      }
    }
}


int main() {
    // Generate some random key-value pairs to bulk-load the Dynamic PGM-index
    std::vector<std::pair<uint32_t, uint32_t>> data(1000000);
    std::generate(data.begin(), data.end(), [] { return std::make_pair(std::rand(), std::rand()); });
    std::sort(data.begin(), data.end());

    // Construct and bulk-load the Dynamic PGM-index
    pgm::DynamicPGMIndex<uint32_t, uint32_t> dynamic_pgm(data.begin(), data.end());
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; i++) {
        threads[i] = std::thread(do_inserts_and_contains, &dynamic_pgm);
    }

    return 0;
}