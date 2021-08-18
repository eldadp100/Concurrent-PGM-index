

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
    template<typename L> // L for Level
    class EBR {

        private:
        int epoch = 0;
        std::map<size_t, int> threads_epochs; // mapping between tid to their current seen epoch
        std::mutex lock; // ensures sequential behavior
        std::vector<L*> buckets[3]; //the 3 buckets to keep the not deleted yet elements - of epoch, epoch-1, "epoch-2". current bucket is epoch % 3
        float update_epoch_check_prob;

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
                    for (L* l : buckets[tmp_epoch % 3]) {
                        l->clear(); // free the levels
                    }
                    buckets[tmp_epoch % 3].clear(); // clear the pointers also
                    epoch = tmp_epoch + 1;
                    // no need to do atomic operation because this thread remains at tmp_epoch until it returns.
                    // epoch may already increase by at most 1 until we update.
                }
            }
            lock.unlock();

        }


        void delete_level(L *l) {
            lock.lock();
            buckets[epoch % 3].push_back(l);
            lock.unlock();
        }

    }
}