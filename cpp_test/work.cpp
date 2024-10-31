#include <iostream>
#include <vector>

using namespace std;

/* 在keys中寻找大于等于key的最小下标 */
size_t binarySearch(const vector<int> &vec, int key) {
  if(key < vec[1]) {
    return 0;
  }
  size_t l = 1, r = vec.size(), mid;
  while (l <= r) {
    mid = l + (r - l) / 2;
    if(key >= vec[mid]) {
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }
  return l - 1;
}

size_t binarySearch2(const vector<int> &vec, int key) {
  int l = 1;
  int r = vec.size();
  int mid;
  while (l < r) {
    mid = l + (r - l) / 2;
    if (vec[mid] > key) {
      r = mid;
    } else if (vec[mid] < key) {
      l = mid + 1;
    } else {
      return -1;
    }
  }
  return l;
}


bool insert(vector<int> &vec, int key) {
  size_t pos = binarySearch(vec, key);
  cout << pos << endl;
  if (pos <= vec.size() && vec[pos] == key) {
    return false; // 发现重复值
  }

  if(pos == vec.size()) {
    vec.push_back(key);
    return true;
  }
  vec.push_back(0);
  for (size_t i = vec.size(); i > pos + 1; --i) {
    vec[i] = vec[i - 1];
  }
  vec[pos + 1] = key;
  
  return true;
}

int main() {
  int size_ = 5;
  int key = 20;
  vector<int> vec = {-100, 2, 6, 9, 11};
  cout << binarySearch2(vec, key) << endl;
  cout << binarySearch(vec, key) << endl;
  // insert(vec, key);
  // for(auto &e : vec) {
  //   cout << e << " ";
  // } 
  // cout << endl;
  return 0;
}