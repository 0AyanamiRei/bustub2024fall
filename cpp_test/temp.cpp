#include <iostream>
#include <tuple>
#include <cstddef>

struct TupleMeta {
  char data[16]; // 假设 TupleMeta 的大小为 16 字节
};

using TupleInfo = std::tuple<uint16_t, uint16_t, TupleMeta>;

int main() {
  std::cout << "sizeof(TupleInfo): " << sizeof(TupleInfo) << std::endl;
  std::cout << "alignof(TupleInfo): " << alignof(TupleInfo) << std::endl;

  TupleInfo tuple;
  std::cout << "Address of std::get<0>(tuple): " << static_cast<const void*>(&std::get<0>(tuple)) << std::endl;
  std::cout << "Address of std::get<1>(tuple): " << static_cast<const void*>(&std::get<1>(tuple)) << std::endl;
  std::cout << "Address of std::get<2>(tuple): " << static_cast<const void*>(&std::get<2>(tuple)) << std::endl;

  // 计算每个元素的偏移量
  std::cout << "Offset of element 0: " << reinterpret_cast<const char*>(&std::get<0>(tuple)) - reinterpret_cast<const char*>(&tuple) << std::endl;
  std::cout << "Offset of element 1: " << reinterpret_cast<const char*>(&std::get<1>(tuple)) - reinterpret_cast<const char*>(&tuple) << std::endl;
  std::cout << "Offset of element 2: " << reinterpret_cast<const char*>(&std::get<2>(tuple)) - reinterpret_cast<const char*>(&tuple) << std::endl;

  return 0;
}