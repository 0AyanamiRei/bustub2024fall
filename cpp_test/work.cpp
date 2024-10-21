#include <iostream>
#include <vector>

using namespace std;

/* 在keys中寻找大于等于key的最小下标 */
size_t binarySearch(const vector<int> &vec, int key) {
  size_t l = 0, r = vec.size(), mid;
  while (l < r) {
    mid = l + (r - l) / 2;
    if (vec[mid] < key) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return l;
}

bool insert(vector<int> &vec, int key) {
  size_t pos = binarySearch(vec, key);
  if (pos < vec.size() && vec[pos] == key) {
    return false; // 发现重复值
  }
  vec.push_back(0);
  for (size_t i = vec.size() - 1; i > pos; --i) {
    vec[i] = vec[i - 1];
  }
  vec[pos] = key;
  
  return true;
}

int main() {
  int max_size = 10;
  int array_[10] = {1, 2, 4, 5, 6, 7, 11, 18, 19, 20};
  int right_array[10];
  int sz = 0;
  int left_nums = 11/2;
  int pos = 2;
  int newkv = 3;

  for (int i = left_nums - 1; i < max_size; i++) {
    right_array[sz++] = array_[i];
  }
  // 左节点等价于新插入kv
  for (int i = left_nums; i > pos; --i) {
    array_[i] = array_[i - 1];
  }
  array_[pos] = newkv;

  for(int i = 0; i < sz; i ++) cout << right_array[i] << " ";
  cout << endl;
  for(int i = 0; i < left_nums; i++) cout << array_[i] <<" ";

  return 0;
}