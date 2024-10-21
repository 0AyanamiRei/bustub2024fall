# BOOK

## Buffer Manager

>> 替换策略: `LRU`

> Pinned blocks
>> `pin()`, `unpin()`, `pin count`计数

> lock
>> 对Shared and Exclusive Locks的期待, 允许多个进程持有共享锁, 表明一起读某段数据, 只允许单个进程持有独占锁, 且这个时候其他进程不得持有共享锁表明正在对这段数据进行修改, 一个实际的例子是, 某段资源被持续的读取, 当前时间点多个进程持有共享锁, 然后有个进程请求访问独占锁, 这个时间点后不得有进程再获取共享锁, 同时该进程需要等待已被分配的共享锁释放后再获取独占锁进而修改数据.

> lock和pin的使用规则
>> 重点是思考lock和pin的交互, 即使用次序


> Buffer-Replacement Strategies

一个体现出DB与OS的不同的例子, 假设存储`instructor`和`department`的文件不是同一个

```sql
select *
from instructor natural join department;
```

下面是假设其对应的伪代码版本

```
for each tuple i of instructor do
  for each tuple d of department do
    if i[dept_name] = d[dept_name]
      then begin
      let x be a tuple defined as follows:
      x[ID] := i[ID]
      x[dept_name] := i[dept_name]
      x[name] := i[name]
      x[salary] := i[salary]
      x[building] := d[building]
      x[budget] := d[budget]
      include tuple x as part of result of instructor ⋈ department
    end
  end
end
```

设想`instructor`中的*tuple*和`department`中的*tuple*的使用次序

> Journaling file systems

# LEC

在DB中的一些名词和OS区分开, `latches`和`locks`不同, `page table`通常是一个哈希表, 跟踪*buffer pool*中存在的*pages*. 

同时DB自身的名词也有容易混淆的, `page table`和`page directory`, 页目录是存储关于*page id*和*page*在db文件中位置的映射, 或者说是记录的*page*在磁盘中的位置, 这本身是需要保存在磁盘, 以便整个DB重启, 而页表, 是存储*page id*和*page*拷贝的位置的映射, 即在*buffer pool*中的拷贝.

## Buffer Pool Optimizations

### Multiple Buffer Pools

就像是xv6中多个允许并发使用的哈希桶一样, 为了降低*latche*冲突, 以及提供并发性, 我们允许存在多个*buffer pool*, 可以是为每一种类型的*page*建立一个*buffer pool*, 或者也可以仅仅是拥有多个*buffer pool*, 都可以, 取决于设计者.

***选取缓存池***

存在多个缓存池, 自然就有了更多的麻烦, 至少在设计上我们要解决一些疑问:

1. 如何管理这些缓存池, 哪些*metadata*是需要的?

一个缓存池的大小是否静态固定的?  有没有根据应用场景自适应调整缓存池大小的说法, 比如我们为不同*page*开不同的缓存池, 那么有的时候*index page*多, 有的时候*data page*多, 或者是*metadata page*, 也许我们可以指定一个总size, 然后交给数据库自己去根据哪些*page*多, 分配更多的空间到对应的缓存池.

根据*LEC*得到的答案是多个缓存池的大小的确是固定的, 不能自动调整.  有人指出这存在空间的浪费, 比如用户指定了10GB的缓存池, 但是仅仅是分配给了数据库, 却没有使用. 这种情况*Andy*把问题放在了使用者身上, 也就是这种责任不应当由数据库承担, 用户应当理解自己的行为是什么样的结果.

2. 怎么为不同的*page*分配对应的缓存池

添加额外的标识符信息, 在*page_id*上加一些bit的使用? 也可以在原有*page_id*基础上, 根据*hash()*得到的值来选取. 如果说根据不同类型的*page*来选择缓存池, 那么我们额外在*page*的*metadata*中添加一些`page type`的信息就可以. 

有人提问是否可能不同缓存池持有同一个*page*, 我想这应该是不允许, 这大概在这一层增加了一致性的困难, 而本可以避免.

### Pre-fetch

由于*SQL*是一种声明式的语言, 所以在之前看到的**查询引擎**, 能够知道具体需要查询哪些*page*, 以此可以做一些预取*page*的操作, 但是也有一些情况难以进行, 比如下面的语句:

```sql
SELECT * FROM A
WHERE val BETWEEN 100 AND 250
```

### Scan-sharing

扫描*page*并读取到缓存池的那个东西我们称为`cursor`, *scan-sharing*就是允许不同的事务放置同一个`cursor`在一个*page*上, 具体来说就是事务A的`cursor`正在扫描page#4~page#5, 这个时候启动事务B, 发现事务B也需要这些page的内容, 那么事务B也将`cursor`放在page#4上, 先跟着事务A一起扫描#4~#5, 再回过头去读取自己另需的page


>> If a query wants to scan a table and another query  is already doing this, then the DBMS will attach  the second query's cursor to the existing cursor.

### Bypass

## Buffer Replacement Policies

