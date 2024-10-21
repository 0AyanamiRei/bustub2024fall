#include <iostream>
#include <optional>
#include <cstring>
#include <memory>

using std::cout, std::endl;

struct A{

  A() {cout << "constructor" << endl; }
  A(A &&that) {cout << "move construct" << endl; }
  A& operator=(A &&that) {
    cout << "move assignment" << endl;
    return *this;
  }
};

struct B{
  B(){}
  auto Foo() -> std::optional<A> {
    std::optional<A> a;
    return a;
  }
};

int main(void) {
  B b;
  auto a = b.Foo();
  return 0;
}