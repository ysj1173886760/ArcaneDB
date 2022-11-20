# ArcaneDB

目前名字还没有确定，随意拍了一个叫做ArcaneDB，下文就统一用ArcaneDB

## 目标

数据模型为有向属性图，即用户可以定义点边上属性的Schema

ArcaneDB的定位是图数据库的存储引擎，提供对于点边的读写原语。

对外提供的接口为：

* GetOneHop -> 读取一个顶点的所有出边
* GetPoint -> 读取一个顶点
* InsertEdge/UpdateEdge/DeleteEdge -> 插入/更新/删除一条边
* InsertPoint/UpdatePoint/DeletePoint -> 插入/更新/删除一个点

ArcaneDB可以作为分布式图数据库的存储层，计算层可以构建在存储层之上，并基于存储层提供的原语实现：

* 图查询语言的解析（OpenCypher，Gremlin）
* 生成分布式执行算子（多跳查询等）

整体目标为实现高性能的单机存储引擎，并提供基本的读写原语。并且在设计与实现的过程中，保持演进的能力，以便将来的拓展。

## 整体架构

![](https://picsheep.oss-cn-beijing.aliyuncs.com/pic/20221120155828.png)

整个存储引擎分为3个模块，分别为LogStore, PageStore, 以及BTree。并对外提供两个逻辑的抽象，分别为EdgeStorage和VertexStorage。

下面介绍一下这些名词：

* EdgeStorage：存储一个顶点的所有出边
* VertexStorage：存储一个顶点
* BTree：将数据组织称BTree的形式存储，包含若干个子模块
* BufferPool：控制BTreePage的换入换出
* TxnManager：负责事务的并发控制
* SchemaManager：负责管理Schema
* DirtyList：负责将修改过的BTreePage刷回到磁盘中
* LogStore：日志相关模块，负责写入WAL
* PageStore：负责存储BTreePage

### 演进方向

#### The Log Is The Database

其实LogStore和PageStore也是用来为BTree服务的，之所以将他们单独拆开，是为了方便后续的演进。

因为LogStore存储Page的WAL，PageStore存储的Page本身。我们可以将LogStore以及PageStore从BTree模块分离开，放到单独的存储节点中。存储节点负责在Page上重放WAL，并提供`ReadPage`和`WriteLog`的接口，整体思路参考Amazon Aurora

存算分离后，ArcaneDB BTree层和PageStore层可以独立任意Scale out。

并且PageStore层还可以架在共享存储之上，比如S3。因为PageStore重放Page，需要消耗的资源是CPU和内存，可以将磁盘层再独立出来。

![](https://picsheep.oss-cn-beijing.aliyuncs.com/pic/20221120161338.png)

* 计算层负责分布式算子的执行，以及图查询语言的解析，主要消耗的资源为CPU
* BTree层负责组织内存中的BTree，主要消耗的资源为内存
* PageStore层负责在Page上重放WAL，主要消耗的资源为CPU
* 磁盘层负责存储具体的数据，主要消耗的资源为磁盘

#### 物化视图

既然Log is Database（Source of Truth），那么其他形式的数据都是Log的一种物化形式。

因为Log可以看作是对数据库状态变更的记录，那么上层的数据组织形式（BTree，LSM）都是一种视图。我们可以确定这种物化的形式，并加速特定的查询。

这里引出的一个思路就是LogStore可以产出一条binlog流以便其他组件订阅。比如我们希望提供高效的多跳查询，就可以额外构建一套计算引擎来订阅binlog流。ArcaneDB主要负责TP，而这一套新的计算引擎则可以物化binlog流来加速多跳查询，负责Serving

![](https://picsheep.oss-cn-beijing.aliyuncs.com/pic/20221120163930.png)

ServingEngine可以用流式计算引擎，消费binlog并产出物化视图。上层的计算节点可以利用ServingEngine的高性能的多跳查询能力完成自己的计算。