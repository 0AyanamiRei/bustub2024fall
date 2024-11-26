#include <iostream>
#include <queue>
#include <vector>
#include <unordered_map>
#include <set>

using namespace std;

int main() {
    std::set<int> s;

    s.insert(10);
    s.insert(5);
    s.insert(20);

    std::cout << "Set elements: ";
    for (const auto& elem : s) {
        std::cout << elem << " ";
    }
    std::cout << std::endl;

    auto it = s.find(5);
    s.erase(10);
    auto min_val = *s.begin(); // 最小值
    auto max_val = *s.rbegin(); // 最大值

    return 0;
}