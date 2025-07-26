## Storage

### ***flash note day1***

主要是源码, 存储部分看了个大概, 细节操作不需要了解太多

1. tuple的metadata并没有内嵌到tuple中, 交给`TablePage`的`tuple_info_`(`Slot_Array`)来管理
2. page, tuple粒度没有并发控制, 交给table_heap做事务相关的并发的控制, 因此插入tuple这种操作, 在heap这也有对外的接口(并发安全版本)
3. bustub的`Slotted Page`没有对删除后的tuple进行处理, 只是记录了page内被删除tuple的数量, 以及打上了标签

**Slotted-Page** vs **Log-Structured**

Slotted-Page Storage的缺陷
- Fragmentation
- Useless Disk I/O
- Random Disk I/O (tuple分布在不同page)

Log-Structured Storage的缺陷
- Write-Amplification
- Compaction is Expensive

B+Tree pays maintenance costs upfront, whereas LSMs pay for it later.

### ***flash note day2***

***topic: AP vs TP, NSM vs DSM***

TP, NSM的summary:
- 写操作快: Fast inserts, updates, and deletes
- Good for queries that need the entire tuple
- Can use index-oriented physical storage for clustering
- 不适合扫描表的某个子属性
- 访问模式糟糕的内存局部性差
- 不适合数据压缩

AP, DSM的summary:
- 减少不必要的IO
- 更快的查询执行, 有良好的局部性和cache复用率
- 适合数据压缩: dictionary compression
- 点查询慢, 考虑到某个entry的tuple : splitting/stitching/reorganization

DSM separate pages per attribute

***PAX?(Partition Attributes Across)***

下一个主题是数据压缩, 看ppt的图更好理解: https://15445.courses.cs.cmu.edu/fall2024/slides/05-storage3.pdf

> The DBMS can compress pages to increase the utility of the data moved per I/O operation. Key trade-off is speed vs. compression ratio

1. 定长value, 可变长数据存储到单独的pool中
2. 查询执行中尽可能延迟解压---`late materialization`
3. 无损

压缩的粒度又是不同的抉择:block level, tuple level, Attribute level, column level

***bit packing***

```text
13:  00000000 00000000 00000000 00001101
92:  00000000 00000000 00000000 01011100
172: 00000000 00000000 00000000 10101100
```

可以压缩为:

```text
13:  00001101
92:  01011100
172: 10101100
```

但是这样的压缩方式如何决策? 如果现在压缩了, 能保证这一批数据以后不会存一个更大的吗? 
做一点取舍如果大批量数据能用8bit存储, 超过其上限的数据能接受截断吗? 或者打个patch, 将这些数据单独存一份

***bitmap encoding***

和os里管理磁盘的bitmap有所区别, 不要混淆了, os里的bitmap应该叫bitmap indexes.

比如Class字段有445, 721, 799三个值, 直接记录至少需要一个int32大小的存储, 如果维护3个bitmap, 那么一个entry只需要维护3个bits, 但是这种方法的局限在于如果字段的基数太多, 比如1~1000, 那么就需要维护1000个bitmap, 这种情况下不适用.  

拓展: Roaring Bitmap, 解决上面描述的不适用情形

- https://roaringbitmap.org/about/
- https://zhuanlan.zhihu.com/p/1894443498750604133

***delta encoding***

看图是最直接的, 结合RLE能压缩很大的空间.

**dictionary encode**

应该是最常用的, 对某一列的数据, 维护一个能表示该列所有数值的字典, 用key替换掉原数据

## bpm

### ***flash note day3***

why not mmap?

- 无法控制刷盘时机, 破坏事务原子性(如果需要的话)
- page fault带来的IO停顿会阻塞线程 影响吞吐量
- OS管理的页表还有其他进程使用, 多核情况下成为bottleneck

缓存page淘汰策略 lru和clock, 主要问题是 `sequential flooding`


## 调优记录

提交文件:
1. src/include/buffer/lru_k_replacer.h
2. src/buffer/lru_k_replacer.cpp
3. src/include/buffer/buffer_pool_manager.h
4. src/buffer/buffer_pool_manager.cpp
5. src/include/storage/disk/disk_scheduler.h
6. src/storage/disk/disk_scheduler.cpp
7. src/storage/page/page_guard.cpp
8. src/include/storage/page/page_guard.h
9. GRADESCOPE.md



> ./bin/bustub-bpm-bench --latency 1
scan: 20047.028630470286
get: 4189.481051894811
cache hint: 19213/692525