#include <iostream>
#include <string>

using namespace std;

/**
 * PrintMerge()和PrintMerge_()的区别体现在传参为右值的时候
 * 前者
 *
 *
 */

template <typename T>
void PrintMerge(T &&a, T &&b) {
  cout << std::forward<T>(a) + std::forward<T>(b) << endl;
}

template <typename T>
void PrintMerge_(T &&a, T &&b) {
  cout << a + b << endl;
}

int main() {
  int a = 10, b = 20;
  PrintMerge(a, b);  // 这里传参为左值 即使函数参数是右值, 但是调用者传的是左值, 所以依旧是左值
  std::string s1{"one "};
  std::string s2{"two "};
  PrintMerge(std::move(s1), std::move(s2));   // 这里传参为右值
  PrintMerge_(std::move(s1), std::move(s2));  // 这里传参为右值

  return 0;
}