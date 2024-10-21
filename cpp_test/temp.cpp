#include <readerwriterqueue/readerwriterqueue.h>
#include <iostream>
#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <queue>
#include <utility>
#include <thread>
#include <future>
#include <optional>

using namespace std;

class MyFuture {
 public:
  MyFuture(std::shared_ptr<moodycamel::BlockingReaderWriterQueue<bool>> q) : q_(std::move(q)) {}  // NOLINT
  MyFuture(const MyFuture &) = delete;                                                         // NOLINT
  MyFuture &operator=(const MyFuture &) = delete;                                              // NOLINT
  MyFuture(MyFuture &&other) noexcept : q_(std::move(other.q_)) {}                             // NOLINT
  MyFuture &operator=(MyFuture &&other) noexcept {                                             // NOLINT
    if (this != &other) {
      q_ = std::move(other.q_);
    }
    return *this;
  }
  bool get() {  // NOLINT
    bool value;
    q_->wait_dequeue(value);
    return value;
  }

 private:
  std::shared_ptr<moodycamel::BlockingReaderWriterQueue<bool>> q_;
};

class MyPromise {
 public:
  MyPromise() : q_(std::make_shared<moodycamel::BlockingReaderWriterQueue<bool>>()) {}  // NOLINT
  MyPromise(MyPromise &&otr) : q_(std::move(otr.q_)) {}                           // NOLINT
  MyPromise &operator=(MyPromise &&otr) noexcept {                                   // NOLINT
    if (this != &otr) {
      q_ = std::move(otr.q_);
    }
    return *this;
  }
  MyPromise(const MyPromise &) = delete;             // NOLINT
  MyPromise &operator=(const MyPromise &) = delete;  // NOLINT
  void set_value(const bool &value) {                // NOLINT
    q_->enqueue(value);
  }
  MyFuture get_future() {  // NOLINT
    return MyFuture(q_);
  }

 private:
  std::shared_ptr<moodycamel::BlockingReaderWriterQueue<bool>> q_;
  friend class MyFuture;
};

template <class T>
class Channel {
 public:
  Channel() = default;
  ~Channel() = default;
  void Put(T element) {
    std::unique_lock<std::mutex> lk(m_);
    q_.push(std::move(element));
    lk.unlock();
    cv_.notify_all();
  }
  auto Get() -> T {
    std::unique_lock<std::mutex> lk(m_);
    cv_.wait(lk, [&]() { return !q_.empty(); });
    T element = std::move(q_.front());
    q_.pop();
    return element;
  }
  auto Size() -> int { return q_.size(); }
 private:
  std::mutex m_;
  std::condition_variable cv_;
  std::queue<T> q_;
};

#define bk_threads 5

using PROMISE = std::promise<bool>;
using PROMISE = MyPromise;

struct DiskRequest {
  bool is_write_;
  int page_id;
  PROMISE callback_;
};

class DiskScheduler {
public:
  DiskScheduler() {
    cout << "创建DiskScheduler" << endl;
    for(int i = 0; i < bk_threads; i ++) {
      background_thread_[i].emplace([this, i] { StartWorkerThread(request_queue_[i]); });
    }
  }

  ~DiskScheduler(){
    cout << "销毁DiskScheduler" << endl;
    for (auto &r : request_queue_) {
      r.Put(std::nullopt);
    }

    for (auto &t : background_thread_) {
      if (t.has_value()) {
        t->join();
      }
    }
  }

  void Schedule(DiskRequest r) {
    auto index = r.page_id % bk_threads;
    request_queue_[index].Put(std::move(r));
  }

  auto CreatePromise() -> PROMISE { return {}; };

  void StartWorkerThread(Channel<std::optional<DiskRequest>> &request) {
    for(;;) {
      auto r = request.Get();
      if (r == std::nullopt) {
        cout << "结束" << endl;
        return;
      }
      cout << "ioing ...." << endl;
      std::this_thread::sleep_for(std::chrono::seconds(1));
      
      r.value().callback_.set_value(true);
    }
  }

private:
  Channel<std::optional<DiskRequest>> request_queue_[bk_threads];
  std::optional<std::thread> background_thread_[bk_threads];
};



int main() {
  DiskScheduler disk_scheduler;

  auto promise = disk_scheduler.CreatePromise();
  auto future = promise.get_future();
  cout << "准备调用Schedule()" << endl;
  disk_scheduler.Schedule({true, 17, std::move(promise)});
  future.get();
  cout << "IO结束" << endl;
}