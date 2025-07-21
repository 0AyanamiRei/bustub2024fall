## Storage

***flash note day1***

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