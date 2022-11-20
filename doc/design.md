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

## 关键设计

### GlobalIndex

图数据库中，对于边点的访问有一个关键的问题，就是如何定位到具体的边集和点集，对应到ArcaneDB中，就是如何根据请求定位一个EdgeStorage或者VertexStorage。

一个常见的思路就是维护一个全局的索引，比如从用户的顶点ID到系统内部顶点ID的映射。这样我们可以从请求中获取系统内的ID，进而在PageStore中定位到具体的数据页。

这种设计思路的好处就是系统内的ID是内部生成的，可以做成int64格式，这样可以利用很多高性能的内存索引，比如PageStore中维护PageID到Page Address的映射，就可以用ART来实现高效的查询。

而缺点就是出现了一个全局的索引。需要有合理的分区方案，否则可能会影响scale out的能力。并且维护这个索引的开销也是不可忽视的，比如索引的落盘。

在面向scale out的设计思路中，我们可以根据顶点ID划分tablet，每个Tablet的LogStore和PageStore是独立开的。

另一个思路就是PageID为string类型。我们将点边的ID直接编码进PageID中，这样可以直接根据请求获得对应的PageID，并定位到具体的EdgeStorage/VertexStorage。

优点就是避免一次索引，并且实现简单。而缺点则是string类型的PageID占用空间偏多，并且无法使用一些比较厉害的优化。(ART, varint等)

### 数据组织形式

为了支持高性能的写操作，以及RangeScan的能力，使用BTree来组织数据。

在BTree上，通过COW来做并发控制。和常见的Latch Coupling不同，COW可以做到写者不阻塞读者，进而提供更强的读性能。

COW的缺点是每次写入都会复制一份数据，热点写入的情况下会导致拷贝的开销无法忽视，这里的处理方法就是通过GroupCommit的方法来聚合一批的写入请求，进而均摊拷贝的开销。

### 事务

### 存储底座

### PageStore

