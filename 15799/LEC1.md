## BOOK System R

大多数关系型数据库的查询执行遵循迭代器模型，每个物理操作符需要实现三个调用接口
1. Open()：初始化迭代器状态，准备处理
2. GetNext()：生成下一个输出行，或者提示没有更多行
3. Close()：清理迭代器状态

plan tree中的非叶子操作符会对其子操作符调用`GetNext()`，当子操作符指示没有更多行时，父操作符调用`Close()`。这种执行模型称为**pull model**，迭代器模型在每次调用`GetNext()`时会产生较高的函数调用开销，每次处理一行在现代CPU上会导致性能较差。优化的方向有批量输出和代码生成：

1. **Vectorization(向量化)**：通过批量处理使得每次调用获取一批行输出，并利用现代CPU的SIMD指令
2. **code generation(代码生成)**：`generates efficient code from the query execution plan in a language such as C `

关于向量化和编译的权衡可以参考论文：“Everything you always wanted to know about Compiled and Vectorized Queries but were afraid to ask VLDB2018” (关于代码生成的优化点我还不是很懂，以后有机会好好体会一下)

一个优化器可分成四个方向讨论：***Query optimization***、***Search space***、***Cost estimation***、***Search algorithm***，本章主要介绍System R。

名词定义：
- **SPJ** := Select-Project-Join
- **Linear Join** 区分于**Bushy Join**，其构成的plan tree也被叫做**Left-Deep Tree**和**Bushy Tree**(更通用的称呼)，前者形式为$Join(Join(Join(R, S), T), U)$，而后者允许形式如$Join(Join(R, S), Join(T, U))$


System R主要关注SPJ类型查询。其中Select的物理操作符包括`Table Scan`和`Index Scan`，Join的物理操作符包括`Nested Loops Join`和`Merge Join`(要求输入行在join字段有序)。其搜索空间仅包含一系列`Linear Join`，在当时的System R已经会维护一些统计信息；行数、表包含的page数量、index本身使用page数量，每列数据的基数(HyperLogLog算法)。以此来计算单个选择或连接谓词的**选择性**。即所谓的**Cost-based**

System R假设了谓词之间是独立的，这是一个问题，同时搜索空间仅包含left-deep tree，也会忽略掉最优解。

**搜索算法**，System R使用动态规划来找到"最佳"join顺序，假设可以通过将n-1个join的最佳计划扩展一个额外的join来找到n个join的最佳计划，复杂度为$O(n*2^{n-1})$。例如表R,S,T的最佳join顺序Plan(RST)可以考虑从join(R, Plan(ST))、join(S, Plan(RT))、join(T, Plan(RS))中决策出来

**interesting orders**，搜索过程中额外维护的plan属性，以解决一个局部最优的corner case：查询Q涉及三个表的join，join字段为R.a、S.a、T.a，并且假设在S表上根据字段a建有索引。如果简单的进行cost比较，`NLJ(R.a, S.a) with Index(S.a)`的代价小于`MergeJoin(R.a, S.a)`会选择前者，但是后者的结果在a字段上有序，如果再对T.a进行MergeJoin成本显著降低。因此System R会会追踪order属性，优化器在枚举和计算每个子表达式（子查询）的执行计划时，不仅计算该计划的成本，还记录该计划输出结果所具有的interesting orders属性，按照不同字段的有序分为不同组，进行组内选优，最终再选出成本最低的一个plan。即保留多个最优计划，以此来避免错误剪枝。

- 一个最优计划：输出无序
- 另一个最优计划：输出按`a`排序
- 另一个最优计划：输出按`b`排序

## 主要贡献和未来的优化器

System R引入了以下几点策略，同时对优化器的组件构成奠定了基础。

1. 收集统计信息， 基于此对plan的代价进行评估
2. bottom-up的代价评估模型
3. 使用dp搜索最佳join方案，解决基于成本估计的dp search时没有考虑MergeJoin的结果具有有序性，对每个具有有序性输出的plan分组保留最优

不管从哪个角度看，System R都具有局限性，其难以扩展更多的算子和规则。Volcano、Cascades、Starburst这类可扩展优化架构成为了新一代优化器基础。

## LEC

最吸引的我两个话题：

***多查询优化***，Single Query和Multiple Queries的优化, 讨论query优化的时候，我们通常把视角放在单个query上，而如果说系统要执行一批查询，那么可以对这些查询进行整体优化(目前还没有商业数据库有实现，仍然处于研究中)
- https://www.ppmy.cn/server/35287.html?action=onClick
- 动态查询聚类（Dynamic Query Clustering, DQC）: 将同时到达的查询按数据重叠度分组，合并扫描操作。[pGMSQ](https://www.jos.org.cn/jos/article/abstract/3665)
- 图数据批量关键词查询（Batch Keyword Queries on Graphs）: 将包含相同关键词的多个查询合并处理，重用中间结果（如r-clique）。Efficient Batch Processing for Multiple Keyword Queries on Graph Data
- 关键词："Multi-Query Optimization"、"Shared Scan"、"Batch Query Processing"

***湖仓一体的系统或者云端环境下的QO***，在这种环境下，可能没有任何关于数据的统计信息，比如有人将文件上传到S3中，然后要求基于这些文件进行查询，在这种场景下如何进行查询？ 这种场景下的优化即自适应技术发挥作用的地方 √




