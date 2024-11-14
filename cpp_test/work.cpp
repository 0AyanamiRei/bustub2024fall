#include <iostream>
#include <vector>

#define DISALLOW_COPY(cname)                                    \
  cname(const cname &) = delete;                   /* NOLINT */ \
  auto operator=(const cname &)->cname & = delete; /* NOLINT */

#define DISALLOW_MOVE(cname)                               \
  cname(cname &&) = delete;                   /* NOLINT */ \
  auto operator=(cname &&)->cname & = delete; /* NOLINT */

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

using namespace std;

class TableIterator {
 public:
  DISALLOW_COPY(TableIterator);
  TableIterator(int id) : id_(id) {}
  TableIterator(TableIterator &&) = default;
  ~TableIterator() = default;
 private:
  int id_;
};

auto MakeIterator() -> TableIterator {
  return {0};
}

int main() {
  TableIterator t1(1), t2(2);
  auto t_move = std::move(t1);
  t_move = std::move(t2);
  return 0;
}