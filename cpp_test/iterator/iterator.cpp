#include <iostream>
#include <vector>

#define n 3

struct array_{
  int size=0;
  int key[n];
  array_ *next;
};

// 自定义迭代器类
class MyIterator {
public:
  // 构造函数
  MyIterator(int* ptr) : m_ptr(ptr) {}

  // 解引用操作符重载
  int& operator*() const {
    return *m_ptr;
  }

  // 前置递增操作符重载
  MyIterator& operator++() {
    ++m_ptr;
    return *this;
  }

  // 后置递增操作符重载
  MyIterator operator++(int) {
    MyIterator temp = *this;
    ++m_ptr;
    return temp;
  }

  // 相等操作符重载
  bool operator==(const MyIterator& other) const {
    return m_ptr == other.m_ptr;
  }

  // 不等操作符重载
  bool operator!=(const MyIterator& other) const {
    return m_ptr != other.m_ptr;
  }

private:
  int* m_ptr;
};

int main() {
  std::vector<int> numbers = {1, 2, 3, 4, 5}; 

  array_ a1;
  for(int i = 0; i < 2; i ++) a1.key[a1.size++] = i;

  array_ a2;
  for(int i = 2; i < 4; i ++) a2.key[a2.size++] = i;

  array_ a3;
  for(int i = 4; i < 7; i ++) a3.key[a3.size++] = i;

  a1.next = &a2;
  a2.next = &a3;
  a3.next = nullptr;

  // 使用自定义迭代器遍历 vector
  MyIterator begin(numbers.data());
  MyIterator end(numbers.data() + numbers.size());

  for (MyIterator it = begin; it != end; ++it) {
    std::cout << *it << " ";
  }

  return 0;
}