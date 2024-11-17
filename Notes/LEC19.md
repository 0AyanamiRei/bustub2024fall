# BOOK

***recovery algorithms***

1. > Actions taken during normal transaction processing to ensure that enough information
exists to allow recovery from failures.

2. > Actions taken after a failure to recover the database contents to a state that ensures
database consistency, transaction atomicity, and durability.

### 19.3 Recovery and Atomicity

为了满足数据库ACID的特性, 我们要实现下面这些目标:

1. 保障事务对数据库修改的原子性, 需要将修改的内容先放到一个stable storage上, 确保崩溃后能够恢复该事务的修改
2. 为了撤销失败的事务, 我们还需要存储被修改内容的旧值

最常用的技术就是**log-based recovery**, 另外一种是**shadow copying**.

对于写操作的一份日志, 可以用四元组<$T_i$, $X_i$, $V_{new}$, $V_{old}$>描述:

- $T_i$: Transaction identifier
- $X_i$: Data-item identifier, 描述数据的位置, 一般来说就是磁盘block编号和offset之类的
- $V_{new}$, $V_{old}$: 新旧数据

随着一个事务的进行, 其日志中有下面几个标志性的记录:

- <$T_i$ start>: 标志事务$T_i$开始执行的时间点
- <$T_i$ commit>: 标志事务$T_i$已被提交
- <$T_i$ abort> : 标志事务$T_i$已被撤销

---

***19.3.2***

事务对数据的写操作被定义为**对磁盘缓冲区或磁盘本身执行更新**, 业界常用的两个方式是**deferred-modification(延迟修改)**和**immediatemodification(立即修改)**, 顾名思义, 立即修改是说在某一个事务处理过程中对数据进行了修改就立刻应用到数据库中, 而延迟修改则是先利用本地数据副本记录修改的数据, 等到事务提交的时候一起应用到数据库, 两种方法都有各自需要解决的问题:

> **立即修改**需要处理的问题是

- 未提交事务修改的数据被其他事务读取
- 使回滚操作更加复杂, 需要使用详细的日志记录来撤销已应用的修改

> **延迟修改**需要处理的问题是

- 内存开销的增加, 每个事务都维护了各自本地的数据副本
- 提交阶段的开销增加, 事务提交的时候需要应用所有的修改内容

---

***19.3.3***

另外一个问题是处理事务与事务之间的写数据关系, 如果事务T1修改的数据X在T1提取之前被另一个事务T2修改过, 那么撤销T1的时候就会连着T2的修改一同撤销了, 这跟**级联**的概念很接近, 或者说就是级联, 解释一下:

> ***级联回滚(cascading rollback)***, 指的是当一个事务失败时, 其他依赖于该事务修改的事务也被迫回滚的情况. 这种情况通常发生在并发控制机制未能有效隔离事务操作的情况下

因此恢复算法通常要求: **如果一个数据项被一个事务修改, 则在第一个事务提交或中止之前, 其他任何事务都不能修改该数据项**, 不过这应该是事务隔离性的基本要求了.

---

***19.3.5***

利用日志来进行`Redo`和`Undo`事务, **Redo/Undo**过程都比较简单, 而需要强调的是, 重做和撤销操作的顺序需要和事务中进行的顺序保持一致.

> The undo operation not only restores the data items to their old value, but also writes log records to record the updates performed as part of the undo process.

上面这段话描述的是**Redo-Only Logging**, 该日志记录的是undo过程中的数据修改, 这些日志不需要包含被修改数据的旧值, 即$V_{old}$字段, 仅用于redo也就不需要在意旧值, 而成功地undo事务后, 还会往该事务的日志中插入<$T_i$ abort>标志

系统崩溃后, 扫描日志来确定redo和undo的事务, 下面是判断该执行哪种操作的原则:

1. 如果日志仅包含<$T_i$ start>, 说明该事务进行了一半系统就崩溃了, 是不完整的事务, 执行undo来撤销其;
2. 如果日志仅包含<$T_i$ start>和<$T_i$ commit>, 说明日志中存在该事务完整的操作, 执行redo来恢复它;
3. 如果日志在`2.`的基础上还包含了<$T_i$ abort>, 考虑简单的设计, 我们依旧执行redo, 这里就是redo-only日志发挥功能的地方, 存在abort标记说明undo是执行完了的, 那么*Redo-Only Logging*也存在于日志系统中, redo该日志等价于执行undo.  (这里的冗余是先redo了后再undo, 但这简化了恢复算法, 实际效果缩短了恢复的时间)

---

***19.3.6***

**checkpoints**机制, 简化一下日志, 在某个时刻点完整的保存系统的状态, 然后截取此前存储的日志, 以此来缩短崩溃再恢复的时间和压缩日志的大小.

这本书这里写的加检查点操作是阻塞了其他事务的写操作, 也就是没有操作系统里面利用*page fault*来实现后台线程加检查点的行为, 像**模糊检查点**, **ARIES 算法**等技术, 可以减缓加检查点时对系统性能的降低.

### 19.4 Recovery Algorithm

这里的内容比较朴素, 还是描述一下故障恢复算法的具体流程. 主要分为两大阶段: **redo phase**和**undo phase**, 具体来说:

***redo phase***

- 维护数据结构`undo-list`, 最初将`undo-list`设置为日志中`<checkpoint L>`记录的列表L, 然后开始正向扫描日志;
- 遇到任何<$T_i$, $X_i$, $V_{new}$, $V_{old}$> (常规的写操作的日志) 或者<$T_i$, $X_i$, $V_{new}$> (redo-only log) 格式的日志记录时, 执行redo, 将$V_{old}$写到$X_i$处;
- 遇到<$T_i$ start>的记录时, 将$T_i$添加到undo-list中;
- 遇到<$T_i$ abort>或者<$T_i$ commit>时, 从undo-list中移除$T_i$;

上面的规则保障了**重复历史**, 按照事务最初的执行顺序重复所有操作, 最终得到包含所有未完成事务的列表`undo-list`

***undo phase***

系统从日志末尾开始反向扫描日志, 遇到每一个属于`undo-list`的日志记录就执行undo操作. 遇到在undo-list中的事务$T_i$的<$T_i$ start>时, 将<$T_i$ abort>写入日志中, 并将该事务从undo-list中移除.

---

剩下的部分介绍了一下**group commit**, 该技术不是在事务完成后立即强制刷新日志, 而是等待多个事务完成或自事务完成执行以来经过一段时间后, 再一起提交等待的事务组, 估计实际上最大的收益是能够按批执行提交, 组织为顺序的磁盘I/O来加快提交的速度.

### 19.5 Buffer Management

这里讲的主题是: **log buffer的引入**, **log buffer pool与data buffer pool的交互**, **操作系统对缓冲区的影响**, **加检查点的改良: Fuzzy Checkpointing**, 先简单的回顾下buffer, 再仔细讨论加检查点的优化;

这里的讨论应该是建立在*lecture*中讲完**STEAL**和**FORCE**策略后的基础上进行, 正是由于我们选择了steal & no-force的buffer管理策略, 所以依赖undo/redo日志去做处理不一致和故障, 我们引入**log buffer**也和当初引入**buffer pool**是一样的理由, 毕竟我们总不能写一条日志就立刻刷回磁盘, 那样效率太低了.

按照WAL的要求, 日志必须先于数据刷回磁盘, 所以**log buffer pool**和**data buffer pool**的关系如下图

![WAL的log&data要求](/pic/log和data的交互.png)

---

***Early Lock Release***

这里就能看出来在DB领域里, `latch`和`lock`的区别了, 这里`early lock`中的lock, 往往就指的是latch, 在事务里面提取释放锁是一个支持高并行性很重要的条件, 就以B+树为例, 根节点往往被频繁的访问, 而如果每次都在事务提交前一直持有着路径上根节点的锁, 就严重阻塞了其他事务的并行执行.

> Operations acquire lower-level locks while they execute but release them when they complete; the corresponding transaction must however retain a higher-level lock in a two-phase manner to prevent concurrent transactions from executing conflicting actions.

这里提到的*lower-level locks*和*higher-level lock*其实也就是上面描述的*lock*和*latch*

---

***Logical Operations***

区分于**physical operations**, 前者主要对数据库中的数据项进行修改, 这些修改可以简单地通过恢复数据项的旧值来撤销, 而**logical operations**则往往是数据库级别的操作, 涉及到多个数据项的修改, 比如插入,删除以及修改索引等操作, 在这些操作中, 我们往往是短期地持有某些page的锁, 然后迅速的释放掉, 即**Early Lock Release**, 这样就


### 19.5 addition: More Checkpoint Policy

作为补充, 详细的谈一下加检查点的优化.

先从基本的开始, 网上找到的比较满意的资料: https://zhuanlan.zhihu.com/p/546350730

1. **static checkpoint with active Txn**
2. **static checkpoint with active Txn**
3. **fuzzy checkpoint**
4. ...

### 19.9 ARIES

1. **LSN**: log sequence number, 作为一条日志记录的唯一编号, 通过该编号可以快速定位该日志位于哪个日志文件的哪个位置;
2. **pageLSN**: 每个page内嵌的数据, 其指向的日志记录是最后一次修改该page的记录;
3. **flushedLSN**: 在内存中维护, 记录目前flush过的最大LSN, 通俗来讲就是磁盘中最后一条日志记录的LSN; 
4. **recLSN**: 仍然是page内嵌的数据
5. **lastLSN**
6. **MasterRecord**: LSN of latest checkpoint, 存储在磁盘中;

一些rules:

1. 从buffer pool刷一个page回磁盘时, 要求 `pageLSN < flushedLSN`, 即表明该page的日志存储在磁盘上;
2. 当redo/undo某一条日志时, 如果发现`LSN <= pageLSN`, 则abort该日志记录;

![LSN与pageLSN](/pic/LSN.png)

# lecture

> 在开始介绍更多内容之前, 先解释下**STEAL**和**FORCE**

- **Steal policy**: 
  1. 允许数据库系统将未提交的事务修改的page写回磁盘;(换句话说, 将脏页刷回的任务完全交给我们实现的缓存淘汰策略)
  2. 采用Steal策略可以提高系统性能, 它可以在事务未提交的时候就将脏页写回磁盘, 增加可使用的内存空间;
  3. 如果该事务最终被abort, 需要使用日志记录来撤销已写入磁盘的修改;
  4. 大多数系统都采用该策略;
- **Force policy**:
  1. 要求在事务提交之前，必须将该事务所有修改的page都写回磁盘;
  2. 采用该策略可以简化recover的过程, 因为所有已提交修改均保证写入磁盘;
  3. Force策略降低事务提交的性能, 需要等待该事务的所有脏页刷回磁盘;
  4. 大多数系统不采用该策略;

> 仅在这两个策略的抉择的维度上, 我们可以做一些讨论.

`Force+No-Steal`的组合被认为是*trivial*的, 因为这样的系统各方面的系统都非常低, 即使它是安全的.

***1,*** 在Force的维度上看, 如果采取了Force策略, (**收益**)那么可以视为其使得系统满足了ACID中的D:Durability属性, (**付出**)但事务有着非常poor的response time, 因为你需要等待所有脏页刷回磁盘;

***2,*** 在Steal的维度上看, 如果不采取Steal策略, (**付出**)那么buffer pool的空间很快就会被那些正在执行的事务占满, 失去了其作为磁盘缓冲区的功能, 使得整个系统的吞吐量非常的低, (**收益**)而其换来的优势是对于那些需要abort的事务, 我们不需要从磁盘中撤销已写入的修改了.

我们需要的是`No-Force+Steal`的组合, 我们能够获得非常快的事务处理能力和完整的缓冲池, 我个人的理解是对于目前单机的数据库来说, 因为可以乐观地假设系统故障和事务abort这些事件发生的频率比较低, 所以采用`No-Force+Steal`的策略能获得好的收益, 否则在我们引入日志系统来支持故障恢复, 而系统故障发生地又很频繁的情况下, 系统性能也依旧会很差.

新一年的slides上做了说明:

![Force and Steal](/pic/Force&Steal.png)

## log schems

这里讨论了之前看书没有看过的内容, 关于日志具体存储的什么, 在mit6.5840的lab3~4里面, 存储kv对的数据库中看到的只是很简单的一致模式, 现在我们看看关系型数据库的log schems.


