# LEC

## Last LEC

再次回顾, 尽可能的组织一次顺序的磁盘I/O操作的开销比随机I/O好, 可以说是一种设计的方向.


## THIS LEC

基于元组的存储方式(*tuple-oriented storage*)的问题

1. 带来空间碎片
2. 无用的磁盘I/O操作
3. 随机磁盘I/O
4. 环境可能不支持原地更新, 即无法从disk提取一个page, 然后加载到内存进行更新,再写回到原来的位置, 比如Hadoop fs不能进行这种`in-place update`, 而只能进行`appends`

> Log-struct
>> 可以顺序的存放, 只是简单的将新的日志记录追加到page中

> Log-structured Compaction
>> 日志压缩, 过滤掉多余的更新信息

>> 但是反观压缩的过程, 我们将数据重新读回到内存, 再写回到磁盘, 这叫做**Write-Amplification(写放大)**

> Write-Amplification
>> Andy的解释是, 核心思想在于在程序中, 每进行一次逻辑写操作, 比如*insert a tuple*, *update a tuple*, 需要将其读取并写回到磁盘多少次, 而此前讲过的*pagae architecture with slotted pages*不存在该问题

`what if the DBMS could keep tuples sorted automatically using an index?`

> word aligned 问题

>> 可能的坏处

1. Perform Extra Reads
2. Random Reads
3. Reject

>> 解决方案

1. padding
2. reordering

