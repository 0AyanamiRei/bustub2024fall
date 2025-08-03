#include <iostream>
#include <vector>
using namespace std;

const int N = 1e5 + 5;

int main() {
    // n 长度在3到1e5
    // 如果每个数都扫一遍, 1e5*1e5 1e10 > 1s
    // 维护每个数往左和往右的代价
    int n;
    cin >> n;
    vector<int> q;
    for(int i = 0; i < n; i ++) {
      int t;
      cin >> t;
      q.push_back(t);
    }
    
    int left[N], right[N], Rwillbe[N], Lwillbe[N];
    left[0] = 0, Lwillbe[0] = q[0];
    for(int i = 0, add = 0; i + 1 < q.size(); i ++) {
      if (q[i + 1] > q[i] + add) {
        add += 0;
        Lwillbe[i + 1] = q[i + 1] + add;
        left[i + 1] = left[i];
      }
      else if (q[i+1] == q[i] + add) {
        add += 1;
        Lwillbe[i + 1] = q[i + 1] + add;
        left[i+1] = left[i] + add;
      } else { // q[i+1] < q[i] + add
        add += q[i] - q[i+1] + 1;
        Lwillbe[i + 1] = q[i + 1] + add;
        left[i+1] = left[i] + add;
      }
    }

    right[q.size() - 1] = 0;
    for (int i = q.size() - 1, add = 0; i ; i --) {
      if (q[i] < q[i-1]) {
        add += 0;
        Rwillbe[i-1] = q[i-1] + add;
        right[i-1] = right[i];
      }
      else if (q[i] == q[i-1] + add) {
        add += 1;
        Rwillbe[i-1] = q[i-1] + add;
        right[i-1] = right[i] + add;
      } else {
        add += q[i] - q[i-1] + 1;
        Rwillbe[i-1] = q[i-1] + add;
        right[i-1] = right[i] + add;
      }
    }

    int res = 1e9;
    for(int i = 0; i < q.size(); i ++) {
      if (Lwillbe[i] == Rwillbe[i]) res = min(res, left[i] + right[i] - 1);
      else res = min(res, left[i] + right[i]);
    }
    cout << res;

}
// 64 位输出请用 printf("%lld")