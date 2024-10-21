# HyperLogLog

```
Consider the problem of keeping track of the number of unique users accessing a website in a single day. Although this is straightforward with a small site only visited by a few people, it is much more difficult when dealing with a large site with billions of users. In such cases, storing each user in a list and checking for duplicates is impractical. The sheer volume of data leads to significant challenges, including running out of memory, slow processing times, and other inefficiencies.

The HyperLogLog (HLL) is a probablistic data structure that tracks the cardinality of large data sets. HyperLogLog is suited for scenarios like the above, where the goal is to count the number of unique items in a massive data stream without explicitly storing every item. HLL relies on a clever hashing mechanism and a compact data structure to provide accurate estimates of unique users while using only a fraction of the memory required by traditional methods. This makes HLL an essential tool in modern database analytics.
```

`HyperLogLog`是用于解决**统计基数**, 即集合中元素个数的高级数据结构, 有以下参数:

- `b: `哈希值二进制表示的初始位数
- `m: `寄存器/桶的数量, 等于2^b
- `p: `最左边1的位置

其算法如下图:

![HLL](/pic/HLL算法.png)

# Presto’s HLL implementation

在*low-cardinality data*情况下, 获得几乎精确的计数, 如果是*high-cardinality data*则是由**HLL**算法产生的近似估计值

