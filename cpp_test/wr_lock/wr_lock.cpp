#include <mutex>  // NOLINT
#include <shared_mutex>
#include <cstring>
#include <iostream>

#define BUSTUB_PAGE_SIZE 8

using std::cout, std::endl;


class ReaderWriterLatch {
 public:
  void WLock() { mutex_.lock(); }
  void WUnlock() { mutex_.unlock(); }
  void RLock() { mutex_.lock_shared(); }
  void RUnlock() { mutex_.unlock_shared(); }
 private:
  std::shared_mutex mutex_;
};


class Page {
public:
   Page() {
    data_ = new char[BUSTUB_PAGE_SIZE];
    ResetMemory();
  }

  ~Page() { delete[] data_; }

  inline void WLatch() { rwlatch_.WLock(); }
  inline void WUnlatch() { rwlatch_.WUnlock(); }
  inline void RLatch() { rwlatch_.RLock(); }
  inline void RUnlatch() { rwlatch_.RUnlock(); }

private:
  inline void ResetMemory() { memset(data_, 0, BUSTUB_PAGE_SIZE); }
  
  char *data_;
  ReaderWriterLatch rwlatch_;
};



int
main(){

  return 0;
}