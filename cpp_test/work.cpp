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

int comparator(int a, int b) {
  if(a > b) return 1;
  if(a < b) return -1;
  return 0;
}

int get_idx(int keys[], int target, int sz) {
  if(keys[1] > target) { // 属于区间(负无穷, keys[1])
    return 0;
  }

  int l = 1, r = sz, mid;
  while(l < r ) {
    mid = (l+r+1)/2;
    if (target >= keys[mid]) {
      l = mid;
    }  // >=
    else {
      r = mid - 1;
    }  // <
  }
  return l;
}

// keys= {无效值, k1, k2, k3, ..., k_sz}
// 从0~sz开始的区间
// (负无穷, k1), [k1, k2), [k2, k3) ..., [k_sz, 正无穷)
int inner_get_idx(int keys[], int target, int sz) {
  // 第一个区间(负无穷, keys[1])
  if (comparator(target, keys[1]) == -1) {
    return 0;
  }
  // 寻找区间[keys[l], keys[l+1])
  int l = 1, r = sz, mid;
  while (l <= r) {
    mid = l + (r - l) / 2;
    auto res = comparator(target, keys[mid]);
    if (res >= 0) {
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }
  return l - 1;
}

int main() {
  int key = 9;
  int keys[] = {100000, 5, 9};
  cout << inner_get_idx(keys, key, 2) << endl;


  return 0;
}