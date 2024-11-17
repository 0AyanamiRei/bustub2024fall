#include <iostream>
#include <cstring>

using namespace std;


class IndexIterator {
public:
  IndexIterator(int _) {}
  ~IndexIterator() = default;
};

class BPlusTree {
public:
  auto Begin() -> IndexIterator {
    return IndexIterator(1);
  }
};

class Index {
public:
    virtual ~Index() = default;
};

class BPlusTreeIndex : public Index {
public:
  BPlusTreeIndex() {
    container_ = BPlusTree();
  }
  auto GetIter() -> IndexIterator {
    return container_.Begin();
  }
  BPlusTree container_;
};



int main() {
  Index index_ = BPlusTreeIndex();

  auto iter = static_cast<BPlusTreeIndex*>(&index_)->GetIter();
  return 0;
}