# BOOK

> 索引

规定主索引建立在主键上的文件排序为`index-sequential files`

> Sparse Index

Dense Index和Sparse Index的区别在于后者占用的空间更小, 对于DBMS来说, 速度瓶颈在对磁盘的访问中, 选择后者就能减少对磁盘的访问.

> Multilevel Indices

下一个事要考虑的是当关系足够多的时候, 索引文件本身的存储也是一个不小的数字, 同时也为了处理某一个*search-key*对应的数据横跨了几个Blocks, 我们引入了**Multilevel Indices**.

> Secondary indices
> 
更彻底的空间换时间策略


> Indices on Multiple Keys

**复合搜索键(composite search key)**, 设想一个场景, 我们需要查询特定学期/年份注册了特定课程的学生, 那么我们可以通过将元组`(course_id, semester, year)`作为一个*search key*.

> disadvantage of the index-sequential file organization

随着文件的增长, 无论是对索引的查找还是对数据的顺序扫描, 性能都会下降. 虽然可以通过重新组织文件来解决这种性能下降, 但频繁的重新组织是不可取的.

## $B^{+}$-Tree Index Files

> 无重复search-key版本

$B^{+}$-Tree 中, 每个*Leaf Node*与*non-Leaf Node*的结构都是一样的, 可以用结构体`BNode`表示:

```c
struct{
  BNode* P[n];
  Key K[n-1]; 
}
```

是否为叶子节点, 有不同的要求;

>> Leaf Node

对于叶子节点, 要求其容纳的指针数量不低于$\frac{n-1}{2}$, 不超过$n-1$, `P[i]`指向了*search-key* `K[i]`对应的文件

>> non-Leaf Node

对于非叶子节点, 要求其容纳的指针数量不低于$\frac{n}{2}$, 不超过$n$, 此外`P[i]`指向的是树节点.

---

$B^{+}$-Tree维护了*search-key*的有序性:

1. 在左边的叶子节点中记录的*search-key*小于在它右边的叶子节点中记录的.

> 如何处理重复的search-key

容易想到的方法是:

1. 存储多份*search-key*, 允许树上有重复, 但这会给查询和删除操作增加困难;
2. 允许一个节点上`P[i]`存储多个指针, 但这使得树更加复杂, 并且会导致低效的访问;

合理的方案提到了**复合Search-key**, 比如出现多个相同的$A_p$, 我们可以选取适当的$a_i$, 使得($a_i, A_p$)从中能唯一标识.

## Query/Insert/Delete

详情见代码, 这里做复杂度分析

> Query

由于是树型结构, 我们从根节点到叶子节点, 所以总的路径长度不超过$log_{n/2}(N)$个节点, 假设*N*是总记录数量, 一般节点的大小和磁盘块保持一致, 即4KB, 估计参数`n`的值在200到100, 假设`n=100`, 有1e6的*search-key*在一个文件需要查询, 也只需要访问$[log_{50}(1,000,000)]$=4个节点, 因此, 从根节点到叶节点的路径最多需要从磁盘读取四个数据块. 树的根节点通常会被大量访问, 而且很可能在缓冲区中, 因此通常只需要从磁盘读取三个或更少的块.

> Insert

插入有可能会遇到节点指针装满的情况, 这个时候需要进行**分裂(split)**

>> leaf node splitting

>> non-leaf node splitting


> Delete

在此之前, 回顾一下一个节点的参数`n`

1. A node can have a maximum of n ptrs. (i.e. 3)
2. A node can contain a maximum of n - 1 keys. (i.e. 2)
3. A node should have a minimum of ⌈n/2⌉ ptrs. (i.e. 2)
4. A node (except root node) should contain a minimum of ⌈n/2⌉ - 1 keys. (i.e. 1)

在B+树种删除节点的场景:

>> case1 被删除数据所在节点中keys的数量大于最小值

直接删除该值就可以

>> case2

## B+tree's build advice

一个好的B+树构建出来的结果应该是:

1. 一个叶子节点, 或者说一对叶子节点(...)刚好占据到DBMS中的一个page大小, 具体来说和磁盘读取速度和负载类型相关, 如果磁盘速度低, 应对OLAP类型的负载, 节点设计的大一点, 让单次I/O扫描更多的数据; 而如果磁盘速度快, 应对OLTP类型的负载, 经常进行点查询, 那么就让节点设计的小一些, 减少一次I/O读取一个节点的冗余数据(这个时候从大节点中筛选无用数据的成本高过磁盘I/O).
   - HDD: ~1MB
   - SSD: ~10KB
   - In-memory: ~512B
2. 兄弟节点之间在磁盘上的物理位置是紧邻的, 也就是说可以以顺序I/O的方式扫描兄弟节点;

---

有一些针对现实问题的策略:

**Merge Threshold**, 这应该算是常见的策略, 尽可能推迟节点的合并, 一收益的点是我们删除某条数据后往往会伴随着新添加数据;

**Bulk Insert**, 对给定的表建树的策略和快速建堆是一样的, 先将数据按keys排序后再插入, 还有一个策略是一次性要插入n个KV时, 可以先建立一个临时的B+树再merge到原来的树上, 不过怎么merge两颗B+树有待学习.