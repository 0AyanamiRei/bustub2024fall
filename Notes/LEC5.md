# BOOK preview

***data warehouse:*** `a repository (or archive) of information gathered from multiple sources, stored under a unified schema, at a single site. `

将多源的信息集中在一个仓库, 以统一的模式, 单一的地点进行长期存储, 肯定会比从用户从各个地方, 以不同的模式查询数据更优秀. 

1. 提供长期的历史数据
2. 数据的质量和一致性有所保障, *data warehouse*中的数据都是经过清洗和集成的
3. 对使用者来说, 更轻松, 不必处理各式各样的问题, *data warehouse*提供了一个单一整合的数据接口

为事务处理而设计的DBMS和为支持数据仓库系统而设计的DBMS有所不同, 这还是取决于工作负载.

1. 通常的DBMS需要处理数量大, 数据小的查询, 而数据仓库通常是较少的查询, 但是数据量很大;
2. 更重要的是, 数据仓库处理新旧数据的方式, 当旧数据不被需要的时候, 删除它, 也就是没有`update`的操作;
3. 此外数据仓库不必关心并发问题;

`Column-oriented storage` 相比于 ` Row-oriented storage`

***优势***

- 
- 
- 

***缺点***

`hybrid storage`

# LEC