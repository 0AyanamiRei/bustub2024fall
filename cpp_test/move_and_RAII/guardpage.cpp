#include <iostream>
#include <mutex>
#include <queue>
#include <optional>

using namespace std;

struct page {
  int id;
};

class page_guard {
public:
  page_guard(page p_) : p(p_), Isvalid(true) {
    Get();
  }
  page_guard(const page_guard &) = delete;
  auto operator=(const page_guard &) -> page_guard & = delete;
  page_guard(page_guard &&that) noexcept {
    this->p = that.p;
    this->Isvalid = that.Isvalid;

    that.Isvalid = false;
  }
  auto operator=(page_guard &&that) noexcept -> page_guard & {
    if (&that == this) {
      return *this;
    }
    this->p = that.p;
    this->Isvalid = that.Isvalid;

    that.Isvalid = false;
    return *this;
  }
  void Get() {
    printf("page%d 获取锁\n", p.id);
    lk.lock(); 
  }
  void Release() {
    printf("page%d 释放锁\n", p.id);
    lk.unlock();
  }
  ~page_guard() {
    if(!Isvalid) return;
    Isvalid = false;
    Release();
  }
private:
  bool Isvalid;
  struct page p;
  std::mutex lk;
};


struct Context {
  std::optional<page_guard> head;
  std::deque<page_guard> pg_set;
  ~Context() {
    cout << "pg_set: " << pg_set.size() << endl;
    while(!pg_set.empty()) pg_set.pop_back();
    head = std::nullopt;
  }
};

page_guard make_guard_page(int id) {
  return page_guard({id});
}

int main() {
  Context ctx;
  ctx.head = std::move(make_guard_page(-1));
  for(int i = 0; i < 2; i ++) {
    page_guard pg1 = std::move(make_guard_page(i));
    ctx.pg_set.emplace_back(std::move(pg1));
  }
  printf("释放: \n");
}



