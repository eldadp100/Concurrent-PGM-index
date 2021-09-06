

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
#include <map>

namespace pgm {
    template<typename L, typename PGMType> // L for Level
    class EBR {

        private:
        int epoch = 0;
        std::map<size_t, int> threads_epochs; // mapping between tid to their current seen epoch
        std::mutex lock; // ensures sequential behavior
        std::vector<L*> levels_buckets[3]; //the 3 levels_buckets to keep the not deleted yet elements - of epoch, epoch-1, "epoch-2". current bucket is epoch % 3
        std::vector<PGMType*> pgms_buckets[3]; //the 3 levels_buckets to keep the not deleted yet elements - of epoch, epoch-1, "epoch-2". current bucket is epoch % 3
        int CheckEveryNTimes = 10;

        int to_check_counter = 0;

        public:
        void on_start(size_t tid) {
            lock.lock();
            if (threads_epochs.find(tid) != threads_epochs.end()) {
                threads_epochs[tid] = 0;
            }
            to_check_counter = to_check_counter + 1 % CheckEveryNTimes;

            int tmp_epoch = epoch;
            threads_epochs[tid] = tmp_epoch;
            if (to_check_counter == 0) {
                bool b = true;
                for (int i=0; i<threads_epochs.size(); ++i) {
                    if (threads_epochs[tid] != epoch) {
                        b=false;
                    }
                }
                if (b && tmp_epoch==epoch) {
                    for (L* l : levels_buckets[tmp_epoch % 3]) {
                        l->clear(); // free the levels
                        delete l;
                    }
                    for (PGMType *p: pgms_buckets[tmp_epoch % 3]) {
                        // delete p TODO
                        delete p;
                    }
                    levels_buckets[tmp_epoch % 3].clear(); // clear the pointers also
                    pgms_buckets[tmp_epoch % 3].clear(); // clear the pointers also
                    epoch = tmp_epoch + 1;
                    // no need to do atomic operation because this thread remains at tmp_epoch until it returns.
                    // epoch may already increase by at most 1 until we update.
                }
            }
            lock.unlock();

        }


        void delete_level(L *l) {
            lock.lock();
            levels_buckets[epoch % 3].push_back(l);
            lock.unlock();
        }

        void delete_pgm(PGMType *p) {
            lock.lock();
            pgms_buckets[epoch % 3].push_back(p);
            lock.unlock();
        }

    };
}