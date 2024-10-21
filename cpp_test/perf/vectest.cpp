#include <iostream>
#include <vector>

void cpu_intensive_task() {
    double result = 0.0;
    for(int i = 0; i < 100000000; i++) {
        result += i * i;
    }
    std::cout << "Result: " << result << std::endl;
}

void memory_intensive_task() {
    std::vector<int> data;
    for(int i = 0; i < 10000000; i++) {
        data.push_back(i);
    }
    std::cout << "Data size: " << data.size() << std::endl;
}

int main() {
    cpu_intensive_task();
    memory_intensive_task();
    return 0;
}