#include <functional>
#include <iostream>
#include <vector>
#include <string>
#include <memory>

using  namespace std;

enum TypeId { INVALID = 0, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, VARCHAR, TIMESTAMP, VECTOR };

class Tuple {
 private:
  int rid_;
  int data_;
};

class Column {
  friend class Schema;
 private:
  string column_name_;
  TypeId column_type_;
  uint32_t length_;
  uint32_t column_offset_{0};
};

class Schema;
using SchemaRef = std::shared_ptr<const Schema>;
class Schema {
 public:
 // ...
 private:
  uint32_t length_;
  vector<Column> columns_;
};

/**
 * 基类:      PlanNode&Exector
 *          ↓         ↓       ↓
 * 子类: MockScan  IndexScan  Insert   ...
 * 
 * MockScan: MockScanPlanNode, MockScanExecutor
 * IndexScan: IndexScanPlanNode, IndexScanExecutor
 * Insert: InsertPlanNode, InsertExecutor
*/

class PlanNode;
using PlanNodeRef = std::shared_ptr<const PlanNode>;
class PlanNode {
 public:
  PlanNode(SchemaRef output_schema, vector<PlanNodeRef>children)
    : output_schema_(std::move(output_schema)), children_(std::move(children)) {}
  
  SchemaRef output_schema_;
  vector<PlanNodeRef> children_;
};

class MockScanPlanNode : public PlanNode {
 public:
  MockScanPlanNode(SchemaRef output, string table)
    : PlanNode(std::move(output), {}), table_(std::move(table)) {}
 private:
  string table_;
};

class IndexScanPlanNode : public PlanNode {
 public:
  IndexScanPlanNode(SchemaRef output, uint32_t table_oid, uint32_t index_oid)
    : PlanNode(std::move(output), {}),
      table_oid_(table_oid),
      index_oid_(index_oid) {}
  uint32_t table_oid_;
  uint32_t index_oid_;
  // vector<AbstractExpressionRef> pred_keys_;
  // AbstractExpressionRef filter_predicate_;
};

class InsertPlanNode : public PlanNode {
 public:
  InsertPlanNode(SchemaRef output, uint32_t table_oid)
    : PlanNode(std::move(output), {}), table_oid_(table_oid) {}
  uint32_t table_oid_;
};

class Exector {
 public: 
  Exector() {}
  virtual ~Exector() = default;
  virtual void Init() = 0; /**< */
  virtual auto Next(Tuple *tuple, int *rid) -> bool = 0;
  virtual auto GetOutputSchema() const -> const Schema & = 0;
};

class MockScanExecutor : public Exector {
 public:
  MockScanExecutor();
  void Init() override;
  auto Next(Tuple *tuple, int *rid) -> bool override;
  auto GetOutputSchema() const -> const Schema & override;
 private:
  size_t cursor{0};
  size_t size_;
  // <>中的内容表明该成员是一个Tuple func(){}函数
  function<Tuple()> func_;
  const MockScanPlanNode *plan_;
};

class IndexScanExecutor : public Exector {
 public:
  IndexScanExecutor();
  void Init() override;
  auto Next(Tuple *tuple, int *rid) -> bool override;
  auto GetOutputSchema() const -> const Schema & override;
 private:
  const IndexScanPlanNode *plan_;
};

class InsertExecutor : public Exector {
 public:
  InsertExecutor();
  void Init() override;
  auto Next([[maybe_unused]] Tuple *tuple, int *rid) -> bool override;
  auto GetOutputSchema() const -> const Schema & override;
 private:
  const InsertPlanNode *plan_;
};