#include <iostream>
#include <vector>

using namespace std;

enum class TypeId { BOOLEAN, INTEGER };

struct Value {
  union {
    bool boolean_;
    int integer_;
  } value_;
};

class Base {
public:
  virtual ~Base() = default;
  virtual TypeId GetType() const = 0;
  virtual Value GetValue() const = 0;
};

class IntBase : public Base {
public:
  IntBase(int i) {
    value_.value_.integer_ = i;
  }
  ~IntBase() override = default;
  TypeId GetType() const override {
    return TypeId::INTEGER;
  }
  Value GetValue() const override {
    return value_;
  }
private:
  Value value_;
};

class BoolBase : public Base {
public:
  BoolBase(bool i) {
    value_.value_.boolean_ = i;
  }
  ~BoolBase() override = default;
  TypeId GetType() const override {
    return TypeId::BOOLEAN;
  }
  Value GetValue() const override {
    return value_;
  }
private:
  Value value_;
};

int main() {
  std::vector<Base*> base;
  IntBase intbase1(4);
  IntBase intbase2(4);
  BoolBase boolbase1(true);
  BoolBase boolbase2(false);
  base.push_back(&intbase1);
  base.push_back(&boolbase1);
  base.push_back(&intbase2);
  base.push_back(&boolbase2);

  for (auto &b : base) {
    if(b->GetType() == TypeId::BOOLEAN) {
      cout << "BOOLEAN: " << b->GetValue().value_.boolean_ << endl;
    } else if(b->GetType() == TypeId::INTEGER) {
      cout << "INTEGER: " << b->GetValue().value_.integer_ << endl;
    }
  }
}