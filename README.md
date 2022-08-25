#rawdb

单机的KV存储引擎：

1.使用Seastar作为开发框架

2.在flatbuffers基础上自定义RPC协议

3.使用TCP提供服务



实现：

1. 有内存cache。对数据库系统，为了给上层软件提供高效、准确、易用的服务，可以通过创建缓存的方式，减少IO事件的发生概率，将IO事件转换为访存事件，就可提升效率

2. 数据要求持久化到磁盘，并满足可靠性要求:
   1. 事务原子性，即写入磁盘的数据要保证完整性。例如，程序可能在写某个数据页到一半的时候崩溃了，再次启动要能将数据恢复到一个正确的状态
   2. 如果是磁盘坏块导致的数据错误，要能够发现
3. key和value长度可变但不超过2048字节，并实现如下三个功能：
   1. Put(const Slice& key, std::string* value)：将字符串值value关联到key。如果key已经持有该值，则覆写该值。
   2. Get(const Slice& key, const Slice& value)：返回string类型的value值
   3. Delete(const Slice& key)：删除键为key的键值对

4. QPS达到2000
