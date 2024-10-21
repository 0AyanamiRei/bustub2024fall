# file

assumption:

1. no record is larger than a block
2. each record is entirely contained in a single block



Variable-Length Records如何组织文件结构?

***Slotted-page structure***


## Page Storage Architecture (Organization of Records in Files)

书上列举了

- Heap file organization
- Sequential/Sorted file organization (ISAM)
- Multitable clustering file organization
- Tree file organization (usually B+tree)
- Hashing file organization.
## Heap File Organization

> `free-space map` & `second level free-space map`

>> 如果说二级映射记录的是一级映射剩下多少空闲空间这一信息, 可能会因为一级映射记录的数据块的内部碎片导致错误的寻找, 比如一张表可能需要2048KB的内存, 根据二级映射找到的一级映射中每一个数据块的剩余空间都不足2048KB,但是总碎片合计大于2048KB, 这样可能会违背`each record is entirely contained in a single block`的假设.

>> 一种解决方案是仅仅记录一级映射中完全空闲的数据块数量, 这样对于最后一个空闲数据块很大程度上留下一些空闲空间无法使用 (最后一个空闲数据块一旦被使用,在二级映射中记录的数量就变成0)

>> 另外一个使用*free-space map*需要考虑的问题是写入磁盘的频率, 显然对该*map*来说会非常频繁的写入磁盘, 这里的设计是性能和准确性的*trade-off*, 一般可以定期扫描数据库, 重新计算该映射写入磁盘, 不过会因为*outdated*的映射导致错误的检索.

## Sequential File Organization

>> 不太行的一种方式, 使用链表按*search key order*来存放.

## Multitable Clustering File Organization

`Most relational database systems store each relation in a separate file, or a separate set of files. Thus, each file, and as a result, each block, stores records of only one relation, in such a design.`

为了理解这个存放方式, 书上的例子:

```
department relation: 

     dept_name        building           budget
+---------------------------------------------------+
|   Comp. Sci.         Taylor            100000     |
|   Physics            Watson            70000      |
+---------------------------------------------------+

instructor relation:

     ID       name        dept_name       salary
+-------------------------------------------------+
|   10101   Srinivasan    Comp. Sci.      65000   |
|   33456     Gold         Physics        87000   |
|   45565     Katz        Comp. Sci.      75000   |
|   83821    Brandt       Comp. Sci.      92000   |
+-------------------------------------------------+
```

我们将这两种关系*cluster on the key: dept_name*

```
+--------------------------------------------------+......
|Comp. Sci.         Taylor            100000       |  D  .
|  10101   Srinivasan    Comp. Sci.      65000     |  I  .
|  45565     Katz        Comp. Sci.      75000     |  I  .
|  83821    Brandt       Comp. Sci.      92000     |  I  .
|Physics            Watson            70000        |  D  .
|  33456     Gold         Physics        87000     |  I  .
+--------------------------------------------------+......
```

>> multitable clustering的文件组织方式可以加速*join queries*, 但是也会使得其他查询语句变慢, 比如最简单的

```sql
select *
from department
```

