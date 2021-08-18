/*
 * The range lock is a lock that used to lock ranges.
 * The set of locks is [1,N] for some N that dynamically changes.
 * The target is to allow fast lock of a range of the form [1,i] for some i <= N
 * Arbitrary elemetns in [1,N] can be locked and unlocked during the time.
 * */

#include "pgm_index.hpp"
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

namespace pgm {

    class RangeLock {
    public:
        std::atomic<bool> at_range_lock(false);
        ConcurrentHeap<uint8_t, std::mutex*> locks_heap;
        std::vector<std::mutex*> mutexes;
        uint8_t start_index;
        std::mutex *mtx(uint8_t level) { return levels_mtx[level - start_index]; }

        RangeLock(uint8_t _start_index) {
            /* Start index is min_level.
             *
             * */
            start_index = _start_index;
        }

        bool lock_multiple(int end) {
            if (at_range_lock.load() || !CAS(at_range_lock, false, true)) { // the || for optimization
                return false; // already taken. Do something else in the meanwhile
            }
            uint8_t min_key;
            while ((min_key=locks_heap.min().key) < end) {
                mtx(min_key)->lock();
                mtx(min_key)->unlock();
            }
            mtx(end)->lock();
            locks_heap.append(end, NULL);
            return true;
        }

        void let_another_lock() { // unlock until "i-1"
            at_range_lock = false;
        }

        void unlock_one(uint8_t i) {
            mtx(i)->unlock();
            locks_heap.remove(i);
        }
    }
}