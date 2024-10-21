#include "../third_party/readerwriterqueue/readerwritercircularbuffer.h"
#include <thread>
#include <iostream>

using namespace moodycamel;

BlockingReaderWriterCircularBuffer<int> q(10);

int main() {
  // 创建一个生产者线程，它将数据放入队列
  std::thread producer([&]() {
    for (int i = 0; i < 10; ++i) {
      q.wait_enqueue(i);
      std::cout << "Produced: " << i << std::endl;
    }
  });

  // 创建一个消费者线程，它从队列中获取数据
  std::thread consumer([&]() {
    for (int i = 0; i < 10; ++i) {
      int number;
      q.wait_dequeue(number);
      std::cout << "Consumed: " << number << std::endl;
    }
  });

  // 等待两个线程完成
  producer.join();
  consumer.join();

  return 0;
}