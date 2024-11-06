#include <iostream>
#include <vector>

using namespace std;




int main() {

  vector<int> a = {2, 4, 6};
  vector<int> b = {1, 3, 5}; 
  
  if (std::equal(a.begin(), a.end(), b.begin(), b.end(), [](auto &&x, auto &&y) { cout << max (x, y) << endl; ;return x > y;}) ) {
    cout << "true" << endl;
  } else {
    cout << "false" << endl;
  }
  return 0;
}