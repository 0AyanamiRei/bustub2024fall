# Hekaton: SQL Server's Memory-Optimized OLTP Engine

> has a very simple protocol, it came from Microsoft and it showed how to do MVCC along with SQL server which is an on-disk system so Hekaton is an in-memory system. It optimized more further than what we'll talk about for the in-memory case 

notes

- https://fuzhe1989.github.io/2021/05/18/hekaton-sql-servers-memory-optimized-oltp-engine/
- https://zhuanlan.zhihu.com/p/430004838

或许仅仅看看*lecture*上提到**MVCC**需要维护的*tuples*就足够了?

# An Empirical Evaluation of In-Memory MVCC

完整的翻译: https://blog.mrcroxx.com/posts/paper-reading/wu-vldb2017/#1-%E5%BC%95%E8%A8%80

## quick notes

***1. 其他db***

对*Peloton db*有点兴趣, 关键词**self-driving**, **in-memory DB**, 一些资料:

1. https://db.cs.cmu.edu/peloton/
2. https://wyydsb.xin/DB/peloton.html
3. [Peloton的技术背书--2017 VLDB](https://www.vldb.org/pvldb/vol11/p1-menon.pdf)

列存的数据库暂时没有时间调研, 再加上是内存数据库, 需要了解的东西有一些多, 先暂时mark一下

从*Peloton db*催发的其他一些感兴趣的: 

1. [cmu-db group self-driving的另一个(新生命)开源项目](https://github.com/cmu-db/noisepage)
2. [MonetDB: 开源-列式存储-内存数据库](https://github.com/MonetDB/MonetDB)
3. [TUM的HyPer: OLTP/OLAP混合负载-闭源](https://zhuanlan.zhihu.com/p/390448919), 虽然闭源,但是有比较完整的论文列表, 后续也被*Tableau*收购了

***2. 不同的CC策略***

*section3.1~3.4*都做了一些概要, 到时候自己实现某一种的时候再来复习下协议的内容吧.

1. **MVTO** : *MV-Timestamp Ordering*

```
+------------------------------------------------------------------------------+
|   | T17-W                             | T18-R                                |
|---|-----------------------------------|--------------------------------------|
| 1 |                                   | CHECK Txn-ID                         |
| 2 | SET Txn-ID                        |                                      |
| 3 |                                   |                                      |
| 4 |                                   |                                      |
| 5 |                                   |                                      |
| 6 |                                   |                                      |
+------------------------------------------------------------------------------+
```

2. **MVOCC**: *MV-Optimistic Concurrency Control*
3. **MV2PL**: *MV-Two-phase Locking*.
4. **Serialization Certifier** [不懂]

***3. 事务的version storage scheme***

数据结构的部分, 为同一个事务的不同版本维护一个的单向无闩链表: **version chain**;

存储方式: 

1. *Append-only Storage*, 两种方式, 要么O(1)追加到尾部+O(n)查询最新版本, 要么O(n)追加到头部+O(1)查询最新版本, 还有其他考虑因素, 详情还是需要细读;
2. *Time-Travel Storage*, 名字很酷炫, 实质是增加一个数据**master version**, 可以选择最新的版本作为*master version*, 也可以选择最旧的(*SAP HANA*, 以此来提供*SI*快照隔离)
3. *Delta Storage*, 增量存储, 感觉很酷炫, 但是华而不实

***4. version gc***

冗余版本的gc其实好说, 不过好奇的是为什么没有讨论加**checkpoint**.


## unknown topic

1. 3.4中的**Serialization Certifier**和**serial safety net**
2. 5中提到内存式的DBMS可以通过粗粒度**coarse-grained**的基于epoch**epoch-based**的内存管理来追踪被事务创建的版本


## benchmark

探究验证用的负载集: **YCSB**

1. read-only
2. read80%, write20%
3. read20%, write80%

数据的竞争程度通过服从**Zipfian**分布访问tuple来控制, 参数skew表示数据访问的倾斜程度, skew越大表示少量元素被访问的频率越高.

<p align="center">
  <img src="https://user-images.githubusercontent.com/7251387/72624553-7795f900-3947-11ea-8cf2-d53d00cf5196.png">
</p>

模拟现实场景测试的负载集: **TPC-C**, 测量OLTP系统性能的标准负载

### 并发控制协议的测试

实验的方向分为:

1. 扩展性瓶颈 (Scalability Bottlenecks)
2. 事务争用 (Transaction Contention)
3. 异构负载 (Heterogeneous Workload)

扩展性方面的测试配置为YCSB-read-olny, 较低的倾斜程度, 混合事务长度(单事务包含操作数), 扩展线程数量来观察吞吐量变化.

<p align="center">
  <img src="https://blog.mrcroxx.com/posts/paper-reading/wu-vldb2017/figure-6.png">
</p>

事务争用或者说数据竞争方面的测试配置为固定40线程, YCSB-read80%-write20%, YCSB-read20%-write80%, 调整数据访问倾斜程度来观察吞吐量变化.

<p align="center">
  <img src="https://blog.mrcroxx.com/posts/paper-reading/wu-vldb2017/figure-7.png">
</p>


异构负载, 上面执行的测试所有的事务所含读写操作都相同, 该处的异构负载由**读写和只读的SERIALIZABLE事务**混合而成, 实验配置是固定20个线程执行读写事务, 0.8的数据访问倾斜程度, 每个事务100个操作, 每个操作访问不同的tuple, 分别测试带声明"只读"和不带声明的两种情况.

采取高倾斜程度猜测是为了降低线程数量对吞吐量的影响.

<p align="center">
  <img src="https://blog.mrcroxx.com/posts/paper-reading/wu-vldb2017/figure-8.png">
</p>


<p align="center">
  <img src="https://blog.mrcroxx.com/posts/paper-reading/wu-vldb2017/figure-9.png">
</p>
