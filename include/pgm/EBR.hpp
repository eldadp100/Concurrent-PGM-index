#include <map>
#include <random>
#include <iostream>
#define CheckEveryNTimes 10

namespace pgm {

template<typename L> // L is the "Level=vector<Item>" type to store
class EBR {
private:
    int epoch = 0;
    std::map<size_t, int> threads_epochs;
    std::vector<L> buckets[3]; //the 3 buckets 
    uint8_t current_bucket = 0;
    float update_epoch_check_prob;
    
    int to_check_counter = 0;
    
public:

    void on_start(size_t tid) {
      if (threads_epochs.find(tid) != threads_epochs.end()) {
        threads_epochs[tid] = 0;   
      }
      to_check_counter = to_check_counter + 1 % CheckEveryNTimes;

      int tmp_epoch = epoch;
      uint8_t tmp_curr_bucket = current_bucket;
      threads_epochs[tid] = tmp_epoch;
      if (to_check_counter == 0) {
        bool b = true;
        for (int i=0; i<threads_epochs.size(); ++i) {
          if (threads_epochs[tid] != epoch) {
            b=false;
          }
        }
        if (b && tmp_epoch==epoch && tmp_curr_bucket==current_bucket) {
          epoch = tmp_epoch + 1; 
          buckets[current_bucket % 2].clear(); // remove old bucket
          current_bucket = tmp_curr_bucket;
          // no need to do atomic operation because this thread remains at tmp_epoch until it returns.
          // epoch may already increase by at most 1 until we update.
        }
      }
    }


    void delete_level(L l) {
      buckets[current_bucket % 2].push_back(l);
    }

};


}
