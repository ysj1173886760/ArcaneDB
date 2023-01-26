# Txn

事务的并发控制实现起来有这么几个难受的点：

目前希望的是解耦的架构，不太希望事务层去感知数据存储的格式，显式的区分开Transaction Component和Data Component。

这样的话可能会造成一些suboptimal，比如bwtree自己本身不是原地更新的结构，对于多版本的处理会复杂不少。比如一般的mvcc都是在事务提交的时候再写txn ts，需要更新到原本的数据上。在bwtree上要想实现这一点就需要为bwtree的delta加上特殊的commit以及abort标记

并且bwtree的compaction也会和事务的语义耦合起来

并且bwtree本身有多版本的语义，和事务相关的语义耦合起来处理复杂度也会高很多。可能可以参考一下rocksdb的transaction db是怎么做的。

并且page也不仅仅有bwtree page这一种，所以可能出现对这种page比较好的方法，对另一种page就不太行了。这里我还是更希望去实现一个契合RUM假设的系统。

和数据耦合程度最小的并发控制方法就是2PL了。所以这里第一版的设计思路是借鉴google spanner，对于读写事务用2PL来做并发控制。而多版本只用来提供给只读事务。

这里只读事务的ts获取方法参考cicada，每个线程写新版本是通过本地时钟wts，所有线程中wts的最小值作为rts可以为只读事务提供服务。这样只读事务就是完全不阻塞的。

基于2PL可以简化一些实现，比如如果实现的是OCC的话，需要先写intent，做validation，再commit write，这时候如果abort write对于bwtree来说不能覆盖写，处理起来较为复杂。（不希望引入特殊的abort delta，然后让读者去merge，这样会降低读的效率）。基于2PL的话，写入可以写到buffer中，等到commit日志写成功后，再去写入到bwtree中，然后放锁。这样可以让我们在恢复阶段不用做undo。不过这种做法要求我们先写日志，先写日志意味着整体的策略不能做成类似Innodb的样子，即修改page的时候再写日志。

目前想的决策是：
* 2PL处理读写事务，多版本负责只读事务
* 多版本的ReadTs不能通过简单的聚合线程wts来实现，因为当前的执行上下文是bthread。
  * 解决的方法要么是引入epoch，要么是通过txn manager去追踪活跃事务。两个都可以试试。
* 只允许在recovery的时候执行abort。abort的时候没有并发的读者，可以hack一下bwtree

## 一次更新

在目前版本我实现了2PL来处理读写事务，遇到的直接问题是2PL本身不感知多版本中的TS，就导致版本链上的ts不是单调递增的。虽然仍然可以保证读写事务的serializable，以及只读事务的一致性快照，但是对于版本的回收等其他细节问题可能会出现一定的影响。

后续决定转战一些常见的并发控制手段，这里就需要考虑在BwTree上的上锁和Abort机制。

借鉴PG，引入特殊的timestamp。比如0是无效版本，即被abort掉的事务。1是上锁的版本，读者遇到这种版本需要等待。

因为被上锁的版本一定是最新版本，（不允许WW覆盖写），所以abort以及commit的时候不需要写delta以及（page级别的）日志，直接找到对应SortKey的最新版本，更新ts即可。（不过不写日志的话，恢复的时候可能比较困难，无法并行回放page）

为了防止只读事务遇到intent阻塞，这里还是决定追踪所有活跃事务，并且只读事务的read ts只会小于所有活跃事务，从而在遇到intent的时候也会直接跳过。

还有一个点就是，如果提供的是SI，不能简单的选取一个Ts作为读写事务的read ts，因为这时候仍然可能有write ts小于read ts的事务在更新，就导致read ts读到的不是一个一致性的快照。

目前看到的实现（PG，Innodb）都是通过read view来做的。而之前通过实验发现简单的去追踪最小的ts不太靠谱，因为并发度很高。并且在协程的背景下，read view的大小可能是远超core num的，导致read view本身开销较大。

目前来看较为靠谱的方法是用MVOCC。这里就有很多选择了：Hekaton，Cicada等。

这里选择魔改一下Hekaton，让他适配到bwtree中。思路如下：

读请求记录读的sortkey，以及读到版本的ts。

写请求会请求写锁，然后写入到btree中，并且带有Locked标识的ts。对于读后写的处理，有几种，要么是locked ts为txn id，读取的时候判断一下。要么是读取的时候看一下有没有锁，有的话就读上锁的版本。要么是写的时候写到缓存中，读可以直接读写缓存

Hekaton的原始做法是直接写到btree中，读请求遇到锁会去txn manager中找事务状态。这里不希望再多引入一个记录事务状态的全局组件。那么遇到intent就只能spin wait。

如果是发起写请求的时候就要执行写入的话，后续其他事务的读很容易被阻塞。所以决定写入的时候先上锁，然后写缓存。

提交的时候，先将intent写入到btree中，然后拿到commit ts。然后用commit ts去验证读集中的元素是否有变化（版本是否相同）。如果不相同，释放写锁，abort。如果相同，则写入commit log，然后在btree上回写commit ts。

这里写先intent，再拿commit ts，是为了保证所有read ts大于commit ts的人，都可以看到intent，从而serial after当前事务。即保证WR依赖。

对于WW依赖，先上锁的人一定commit ts更小。所以也可以保证。

对于RW依赖，读者如果在写者之前提交，没有问题，如果读者在写者之后提交，那么他可以看到intent，并spin wait，直到写者成功。然后会发现两次读到版本不同，则会abort。

至此可以证明这个算法的正确性。如果spin wait的时间过长，则可以考虑引入speculative read等技术，通过pipeline增强性能。