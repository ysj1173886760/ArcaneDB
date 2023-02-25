# ArcaneDB性能优化

## LockManager

通过采CPU火焰图发现lock manager的锁CPU开销较大。现在怀疑是多核架构下，原子变量的同步时间过长导致的。

所以希望通过去中心化的lock manager来优化。

现在中心化的lock manager（分shard）的写入性能为116万。

![](https://picsheep.oss-cn-beijing.aliyuncs.com/pic/20230225101639.png)

将lock manager做成去中心化的之后，qps上升到了130万左右。平均单核提升了2万左右。