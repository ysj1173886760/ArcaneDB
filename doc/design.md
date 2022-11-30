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
* 不做编码，通过索引定位到邻接表头。

第一种方式需要用户层，或者我们自己额外维护索引，通过属性定位到点边ID。相较于第二种方式并没有节省一次索引，并且还不灵活。所以决定选择第二种方案。

Scan邻接表的数据结构有：链表，LSMT，BTree，数组等。这几种结构Scan的性能逐渐递增，带来的缺点就是写放大会越来越大。

选择折中的BTree来提高Scan性能，并期望通过落盘BwTree的形式来优化写放大。

现在考虑一个用户请求的全链路，以插入一条边举例子：

1. 通过请求的起点ID定位到具体的边集。由于我们是disk-orient的系统，所以这里需要走一个PointID -> PageID的映射
2. 根据PageID定位到磁盘中的数据。即PageID -> DiskOffset

传统的单机数据库中，PageID一般和DiskOffset相关，比如DiskOffset = PageID * PageSize。而定位PageID只需要查找一下对应Table的元数据即可。元数据可以全缓存所以没什么问题。

但是在图数据库中，点非常多，所以PointID -> PageID的映射会变得非常大。同样的，由于每个Point至少对应两个Page，一个存储边，一个存储点，所以在PageID -> DiskOffset的映射中也会变得偏大。

这里的解决思路有两个:

#### 方案1

我们可以通过特殊的编码，将PageID编码成PointID + xxx的形式，这样免除了PageID的分配，以及从PointID到PageID的索引。代价就是PageID为String类型。

后面统称可以定位具体数据位置的信息为PhysicalAddr。

维护PageID -> PhysicalAddr是不可避免的，这个索引有以下的特点：

1. 根据访问的特性，PageID都是点查，没有Scan的需求
2. 不具有局部性，因为上层缓存不命中的时候才会访问这一层
3. value较小
4. 索引规模较大，全内存不可接受

PageID是String类型，并且需要面向磁盘设计，所以选择的点主要就是成熟的LSMT以及BTree。其中value较小比较适合LSMT，因为整体的SST数量以及大小都会偏小，LSMT的写放大会得到缓解。

综上所述，使用落盘的LSMT最适合这种workload。

我们可以通过将PageID哈希成一个int64来缓解PageID的空间开销问题。但是需要处理哈希冲突的情况。一个可选的解决方案就是在Page内部记录原始的Key，然后读取的时候去检查一下原始的Key。如果不匹配就需要重新哈希。这个方法要求我们不能删除原始的Page，相当于将索引Inline到了Page中。不过这个索引比普通的哈希索引冲突会少很多，因为他的范围是int64，而非bucket num。

这样int类型的PageID就可以缓解空间开销问题，下层的PageID -> PhysicalAddr的索引也可以做的更加高效。

#### 方案2

如果针对全内存设计的话，定位到一个点需要point id + type，定位到一个起点的所有出边需要point id + point type + edge type。都是定长int类型，可以通过ART来加速查找。不过具体的开销还需要计算。

这样PointID -> PageID, PageID -> PhysicalAddr这两个索引都可以做成全内存的ART。读取性能会非常高。

但是这两个索引都是需要落盘的。目前没有落盘ART的实现，需要转化成kv对落盘，其中的序列化开销不可忽视。

这里有一个可以探索的点是为这种高性能的基于内存设计的索引支持落盘的能力。

如果不是针对全内存设计的话，第一个索引的访问模式和用户请求相关联，需要通过buffer pool做缓存。但是就会在主链路上引入一定的开销。

### BTree

数据组织的形式使用BTree，在现代SSD的加持下，BTree可以提供高性能的读，以及Scan的能力。

传统的BTree的并发控制手段一般是使用latch coupling，即子节点先上锁，然后父节点放锁。对于同一个Page的访问，读写是互斥的。这就导致高并发的情况下，写操作会影响读操作的性能。

在大多数的场景下，读的数量都是远大于写的，并且图存储引擎希望提供高性能的读（Scan），所以我们希望针对读操作来优化。

写不阻塞读是一个比较常见的话题了，在事务的并发控制手段中，多版本并发控制（MVCC）通过写请求创建新版本的方式来避免读请求被阻塞。一般这里多版本的粒度就是行级。

我们可以使用相似的思路，在写一个BTree Page的时候，创建一个新的副本出来，在上面执行写操作，而读操作则可以直接读取现有的副本。通过CopyOnWrite的方式来避免读请求被阻塞。这样，在一个BTree上的读操作都是永不阻塞的。

对于BTree的SMO操作，我们认为一般是小概率事件，所以会将其和BTree的写操作阻塞，从而简化实现的复杂度。

所以在正常的写操作下，他只会修改BTree中的一个Page。而在SMO的情况下，可能修改若干个Page。

为了防止读性能受到影响，我们允许SMO和读操作的并发，这里的要求是每次SMO会生成新的PageID，老的Page在失去引用后会被删掉。并且要保证子节点先完成SMO，再去完成父节点的SMO。

SMO作为一个系统级别的事务，为了性能以及简化实现，不为SMO操作记录undo日志，即SMO是redo only的。可以类比innodb的mini transaction。

这里希望通过类似BwTree的方式来优化写入能力，即借用BwTree的Delta的思想。将LSM嵌入Btree的节点中。

这里的设计空间有两块，一个是内存中是否选择用Delta，一个是磁盘中是否选择Delta。

对于磁盘来说，使用Delta的好处是用读放大换写放大。因为都是log structured，所以是随机读的iops放大换顺序写的带宽。在现代SSD上，随机读性能不差，但是顺序写可以优化SSD内部的GC。所以一般认为磁盘的Delta是一个比较好的选择，只要iops放大不是太严重就可以。

对于内存来说，使用Delta的好处是写操作可以不用重新写原本的Page，缺点就是读需要Merge-on-read，需要遍历若干个delta来读取数据。一个特别的点在于这个delta的粒度是可以动态控制的，比如一个page是热点写入的情况，我们就可以允许delta多一些，而对于希望优化读性能的场景下，则可以让delta的数量变少，甚至是0个，这样读性能就是最优的。

在任意一种情况下，读写都是相互不阻塞的。在bwtree原始的论文中，允许在同一个page上做无锁的并发写入，虽然lock-free本身性能很高，但是当写入失败的时候，就需要本次写入对应的WAL被abort掉。在高争用的情况下，abort log buffer的开销变得不可忽视，并且bwtree原始的论文中，每次写入只能prepend一个delta的限制使得读操作的间接跳转变得更加严重。所以这里不选择使用bwtree的乐观协议，而是不允许写写并发，并发的写操作会通过GroupCommit聚合起来，由单线程负责写入数据。这种聚合带来两个好处，一个是批量写入log buffer，降低对log buffer模块的压力，第二个是聚合delta使得一个delta中的数据更多，从而减少delta chain的长度。（photondb是通过partial consolidation来减少delta chain的长度）

内存中的consolidation和磁盘中的consolidation的时机是解耦的，缺点是引入一定的空间放大，好处则是允许上述的delta单独处理自己的逻辑（比如consolidation）

### 事务

支持SI

这里有两种思路：

1. 单机下用ReadView做SI，提供XA接口做分布式提交。好处是ReadView的并发度更高，但是分布式场景下需要中心化的节点来维护活跃事务的视图。
2. 每个事务分配StartTs和CommitTs。其中Ts的分配最简单就是TSO，或者用ClockSI，并且用HLC来避免读等待的问题。好处是分布式事务性能会高一些，但是其中ClockSI只能提供generalized SI，不能提供最新的快照。但是单机情况下会有一些不必要的阻塞。

为了拓展性考虑，打算用方案2。（其实也是因为之前有一版方案2的设计了，就直接复用了）

这个也看定位了，假如说ArcaneDB的目标是图数据库中的SQLite的话，很多设计都是可以有一定修改的。

### 存储底座

上面触碰到磁盘的模块就是Log和Page了。Log是AppendOnly的自然不用说。Page的话上面也说到使用类似BwTree的技术，所以也是基于一个LSS（Log Structured Storage）做。

综上存粗底座的唯一需求就是提供AppendOnly的接口。实现的时候只需要抽一个Env出来，然后就可以基于各种环境实现这个Env了，感觉常见的场景就是可以放到S3，Ext4，EBS上。

Env的话就借鉴leveldb的Env就行。主要就是RandomReadFile和AppendOnlyFile。
