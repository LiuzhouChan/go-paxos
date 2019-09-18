# go-paxos - Go 多组 Paxos 库


## 关于
go-paxos 是一个Go实现的多组Paxos共识算法库

## 特性
- 基于Lamport的 Paxos Made Simple 进行工程化，不进行任何算法变种。
- 每次写盘使用fsync严格保证正确性。
- 在一个节点上一次 Propose 的Latency为一次RTT。
- 节点主动向其他节点进行点对点快速学习。

## 待完成
- snapshot 功能的添加。
- 运行过程中节点的增减。
- master 选举。

## 性能
### 运行环境
```
OS: Ubuntu 18.04
CPU: Intel Core i5-7500@ 4x 3.8GHz
内存：8GB
硬盘：Intel P4510 2TB
```
性能测试结果(qps)
```
48个group：37596 
```
## Acknowledgments
- 参考了Go多组Raft算法共识库 https://github.com/lni/dragonboat
- Paxos 协议参考了 https://github.com/Tencent/phxpaxos