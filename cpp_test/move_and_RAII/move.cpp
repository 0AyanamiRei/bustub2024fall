#include <iostream>

using namespace std;

class A {
 public:
  A() = default;
  
  A(A &&rhs) noexcept { cout << "using move construct.\n"; }
  A &operator=(A &&rhs) noexcept {
    cout << "using move assign.\n";
    return *this;
  }
};

void func(A param) {
  cout << "using func.\n";
  A a1;
  a1 = std::move(param);  // 调用移动赋值
}
void test() {
  A a;
  // func(a); //error.
  func(std::move(a));  // 通过移动构造构建临时对象
}

int main() {
  test();
  return 0;
}