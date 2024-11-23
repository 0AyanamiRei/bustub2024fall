#include <vector>
#include <algorithm>
#include <iostream>
#include <deque>
#include <unordered_map>
#include <cstring>

using namespace std;

int main() {
  std::vector<char> data;

  data.resize(4080);
  size_t offset = 0;

  // 打印 data 对象的地址
  std::cout << &data << std::endl;

  // 打印 data 开始的地址
  std::cout << static_cast<void*>(&data[offset]) << std::endl;

  // 向 data 开始的地址写入数据
  const char* message = "Hello, World!";
  std::memcpy(&data[offset], message, std::strlen(message) + 1);

  // 打印写入的数据
  std::cout << &data[offset] << std::endl;


  return 0;
}

/**
 * page_nums = 5
 * (3,1) (6,2) (9,4) (8,7) (5,10)
 * (1,3) (2,6) (4,9) (7,8) (5,10) 1-page页内有序
 *     (1,2)      (4,7)    (5,10)    
 *     (3,6)      (8,9)           2-page跨页有序
 * 
 *          (1,2)          (5,10) 4-page跨页有序
 *          (3,4)
 *          (6,7)
 *          (8,9)
 * 
 *                 (1,2)          8-page跨页有序  4<=5<=8 排序完毕
 *                 (3,4)
 *                 (5,6)
 *                 (7,8)
 *                 (9,10)
 * 
 * 
 ./bin/bustub-sqllogictest ../test/sql/p3.16-sort-limit.slt  --verbose

./bin/bustub-sqllogictest ../test/sql/p3.15-multi-way-hash-join.slt  --verbose 
./bin/bustub-sqllogictest ../test/sql/p3.14-hash-join.slt  --verbose
./bin/bustub-sqllogictest ../test/sql/p3.13-nested-index-join.slt  --verbose 
./bin/bustub-sqllogictest ../test/sql/p3.12-repeat-execute.slt  --verbose 
./bin/bustub-sqllogictest ../test/sql/p3.11-multi-way-join.slt  --verbose 
./bin/bustub-sqllogictest ../test/sql/p3.10-simple-join.slt  --verbose 
./bin/bustub-sqllogictest ../test/sql/p3.09-group-agg-2.slt  --verbose
./bin/bustub-sqllogictest ../test/sql/p3.08-group-agg-1.slt  --verbose 
./bin/bustub-sqllogictest ../test/sql/p3.07-simple-agg.slt  --verbose
./bin/bustub-sqllogictest ../test/sql/p3.06-empty-table.slt --verbose  
./bin/bustub-sqllogictest ../test/sql/p3.05-index-scan-btree.slt --verbose 
./bin/bustub-sqllogictest ../test/sql/p3.04-delete.slt --verbose 
./bin/bustub-sqllogictest ../test/sql/p3.03-update.slt --verbose 
./bin/bustub-sqllogictest ../test/sql/p3.02-insert.slt --verbose 
./bin/bustub-sqllogictest ../test/sql/p3.01-seqscan.slt --verbose 
 * 
*/  

/*
create table t1(v1 int);
insert into t1 values (11), (22), (33), (44), (55), (66), (77), (88), (99);
explain(o)
select * from __mock_table_123, (select * from t1 order by v1 desc limit 3);


*/