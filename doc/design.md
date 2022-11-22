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

## 数据模型

有向属性图

点通过ID + type标识。

边通过起点，终点，边类型来定位，即(起点id，起点type，终点id，终点type，边type)

ID为int64，type为int32

相同type的点具有相同的schema

## 关键设计

### Seek & Scan

图数据的保存一般使用邻接表的方式。图访问对邻接表的操作分为两步，首先要定位到邻接表头。第二步是遍历邻接表，得到具体的点边。

我们称第一步为Seek，第二步为Scan。

![](https://picsheep.oss-cn-beijing.aliyuncs.com/pic/20221121144711.png)

Seek邻接表的做法分为两大类：

* 内部分配点边ID，然后返回给用户，通过数组可以直接索引到邻接表头
* 不做编码，通过索引（ART，LSMT）等结构定位到邻接表头。

第一种方式需要用户层，或者我们自己额外维护索引，通过属性定位到点边ID。相较于第二种方式并没有节省一次索引，并且还不灵活。所以决定选择第二种方案。

Scan邻接表的数据结构有：链表，LSMT，BTree，数组等。这几种结构Scan的性能逐渐递增，带来的缺点就是写放大会越来越大。

选择折中的BTree来提高Scan性能，并期望通过落盘BwTree的形式来优化写放大。

现在考虑一个用户请求的全链路，以插入一条边举例子：

1. 通过请求的起点ID定位到具体的边集。由于我们是disk-orient的系统，所以这里需要走一个PointID -> PageID的映射
2. 根据PageID定位到磁盘中的数据。即PageID -> DiskOffset

一般在云上的环境中，我们基于BlobStore存储的话，需要维护的是PageID到(BlobID, Offset)的映射。

传统的单机数据库中，PageID一般和DiskOffset相关，比如DiskOffset = PageID * PageSize。而定位PageID只需要查找一下对应Table的元数据即可。元数据可以全缓存所以没什么问题。

但是在图数据库中，点非常多，所以PointID -> PageID的映射会变得非常大。同样的，由于每个Point至少对应两个Page，一个存储边，一个存储点，所以在PageID -> DiskOffset的映射中也会变得偏大。

这里的解决思路有两个:

#### 方案1

我们可以通过特殊的编码，将PageID编码成PointID + xxx的形式，这样免除了PageID的分配，以及从PointID到PageID的索引。代价就是PageID为String类型。(hash)

后面称PhysicalAddr为DiskOffset，或者(BlobID, Offset)对应云上环境。

维护PageID -> PhysicalAddr是不可避免的，这个索引有以下的特点：

1. 根据访问的特性，PageID都是点查，没有Scan的需求
2. 不具有局部性，因为上层缓存不命中的时候才会访问这一层
3. value较小
4. 索引规模较大，全内存不可接受

PageID是String类型，并且需要面向磁盘设计，所以选择的点主要就是成熟的LSMT以及BTree。其中value较小比较适合LSMT，因为整体的SST数量以及大小都会偏小，LSMT的写放大会得到缓解。

综上所述，使用落盘的LSMT最适合这种workload。

#### 方案2

如果针对全内存设计的话，定位到一个点需要point id + type，定位到一个起点的所有出边需要point id + point type + edge type。都是定长int类型，可以通过ART来加速查找。不过具体的开销还需要计算。

这样PointID -> PageID, PageID -> PhysicalAddr这两个索引都可以做成全内存的ART。读取性能会非常高。

但是这两个索引都是需要落盘的。目前没有落盘ART的实现，需要转化成kv对落盘，其中的序列化开销不可忽视。

这里有一个可以探索的点是为这种高性能的基于内存设计的索引支持落盘的能力。

如果不是针对全内存设计的话，第一个索引的访问模式和用户请求相关联，需要通过buffer pool做缓存。但是就会在主链路上引入一定的开销。

### 事务

### 存储底座

### PageStore
