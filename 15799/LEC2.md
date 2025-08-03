## Bustub的sql

https://zhuanlan.zhihu.com/p/570917775 文中详细的解释了从libpg_query产生Postgres AST后, 在bustub中如何成为一个可执行物理计划树的, 同时能看出bustub的一些局限.

## Access Path Selection in a Relational Database Management System

论文提出了SQL查询计划的一整套流程思想

1. parser
2. logic optimizer
3. physical optimizer(code generate)
4. execute

在 System R 中，用户无需了解元组的物理存储方式或可用访问路径（例如，哪些列有索引）。SQL 语句不要求用户指定有关元组检索的访问路径信息，也不要求指定连接的执行顺序。System R 的优化器为 SQL 语句中的每个表选择连接顺序和访问路径。在众多可能的选择中，优化器选择执行整个语句的“总访问成本”最低的一个。

单表代价的评估公式：**COST = PAGE FETCHES + W * (RSI CALLS)**

- PAGE FETCHES：描述IO代价，包含data page和index page
- RSI CALLS：表示实际读取的tuples数量
- W：IO cost和CPU cost的权重