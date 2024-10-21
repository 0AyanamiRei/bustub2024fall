#include <iostream>
#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <queue>
#include <utility>
#include <thread>
#include <future>
#include <optional>

using namespace std;

class A {
public:
  A(int i): i(i) { cout << "construct" << i << endl; }
  ~A() { cout << "destory" << i << endl; }
  A(const A &) = delete;
  auto operator=(const A &) -> A & = delete;
  A(A &&that) noexcept = default;
  auto operator=(A &&that) noexcept -> A & = default;

  int i;
};

deque<A> w_set;

int main() {

  int n = 5;
  while(n--) {
    A a(n);
    w_set.emplace_back(std::move(a));
  }

  w_set.pop_front();

  return 0;
}