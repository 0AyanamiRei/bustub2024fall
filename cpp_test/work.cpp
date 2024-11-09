#include <iostream>
#include <vector>
#include <cstdint>
#include <atomic>

using namespace std;

#define INF UINT64_MAX

struct version {
  std::atomic<uint64_t> ver{0};
  uint64_t begin_ts{0};
  uint64_t end_ts{0};
  uint64_t read_ts{0};
  [[maybe_unused]] version *ptr;
  char data;
};

class MVTO {
public:
  MVTO (std::vector<version> &&Q) : Q_(Q) {}
  auto Read(uint64_t ts) -> const char;
  auto Write(uint64_t ts, char data) -> bool;
private:
  std::vector<version> Q_;
};


int main() {
  MVTO mvto({{0, 0, 0, 'A'}});

}

auto MVTO::Read(uint64_t ts) -> const char {

}