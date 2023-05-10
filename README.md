# MIT-6.824

连接：[mit-6.824](https://pdos.csail.mit.edu/6.824/schedule.html)

这是一个刚启动 & 进行中的项目，目前完成了：MapReduce

Most comments in the code are in Chinese. If you need an English translation, please let me know.

Copying my code is not recommended.

## Lab 1: MapReduce

**测试结果**：

```text
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting job count test.
--- job count test: PASS
*** Starting early exit test.
--- early exit test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```

**思路：**

coordinator 是一个 server，worker 向他请求任务，worker 并不知道自己会被分配什么任务，coordinator 执行会维护所有任务的状态，起初他分配 map 任务，所有 map 任务完成后，开始分配 reduce 任务，直至所有任务完成。

对于同一个 woker 会收到两个一样的任务（当然他们派发的时间不同）、worker 收到老旧的曾滞留在网络中任务的问题：

- 分配任务时会告知这是发送给此 worker 的第几个任务，即：`WorkerApplyTaskSeq`
- 所以只有最新的任务会被执行，其余都会被丢弃

对于 coordinator 会收到同一个任务的多个执行结果的问题，

- 告知 coordinator 这是这个任务的第几次分配，即：`TaskAllocSeq`
- 所以只有最新的任务的回复会被接受，其余都会被丢弃

如果 coordinator 发出去的任务超时了（没有在10秒内收到结果），那么这个任务会被重放，参见`taskWaiter(...)`；如果这个任务没有超时，那么他不会再回到任务队列，且会被标记为已完成。在实际的代码中，任务只是标识每个人物的唯一ID，或者说在我的实现中，任务不会动态变化（当然这也是这个实验的本意）。

coordinator 端会维护一个任务队列（线程安全的），可以理解为往队列里放任务和拿任务都是原子的。

只要 coordinator 收到的回复是最新的，那么一定会有个 taskWaiter 在等待（还未超时）。因为如果 taskWaiter 已超时退出，那么这个任务一定已被重放并且它的 `TaskAllocSeq` 一定已被递增，这时即使旧的任务的回复到达，也会因为 `TaskAllocSeq` 检查不通过而被丢弃，不会因为 taskWaiter 退出而被阻塞。

一个例子：

- 0s，A 拿到任务 map-1(0)，任务被标记为`IN_PROGRESS`，启动`taskWaiter`
- 10s，A 超时未完成，导致任务被标记为`IDEL`，并被重放，`taskWaiter`退出
- 10.1s，B 拿到任务 map-1(1)，任务被标记为`IN_PROGRESS`，启动`taskWaiter`
- 11s，
  - 假如 A 先返回，在调用`NotifyTaskComplete`时，会因为`args.TaskAllocSeq == latestSeq`无法通过而被丢弃，`taskWaiter`仍在等待
  - 假如 B 先返回，唤醒  task 对应的 `taskWaiter`，
    - 标记任务为`COMPLETE` ，`taskWaiter`退出

