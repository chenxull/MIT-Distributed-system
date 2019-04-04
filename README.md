# MIT 的分布式系统课程
计划用2个月的时间来学一下这门课程

##Lab1 mapreduce 

**part1**
- 在完成了 doMap 和 doReduce 的功能之后，测试发现，最后输出文件中的内容为空，导致测试失败。最开始定位问题以为子 doReduce 函数中。后来进过不断的 debug 是 doMap函数中，最后存储读取到的文件时发生错误。应该将整个 kv 信息存储到输出文件中供doReduce函数使用，我只存储了 kv.value信息, doReduce函数在最后遍历 keys 无法获取到 key，最后导致结果文件中的数据为空。

**part2**
- 在运行测试的时候，最后 将所有输出文件 merge 的时候，没有找到 `mrtmp.test-res-0`文件。第一个想法是去生成这个文件的找答案。
- map 过程生成的中间数据中就没有编号0的任务。
- task #0丢失，work 节点直接从#1开始执行。在执行任务的时候，是从mrinput-1.txt开始读取的，而不是从0开始读取。


错误原因，在开始执行Map任务时，前二个执行的任务是一样的，跳过了824-mrinput-0.txt的处理。但是在分配任务的过程中，task 已经获取到了824-mrinput-0.txt。

```
/var/tmp/824-501/mr42780-worker0: given Map task #1 on file 824-mrinput-1.txt (nios: 50)
/var/tmp/824-501/mr42780-worker1: given Map task #1 on file 824-mrinput-1.txt (nios: 50) 

```

**失败原因令人震惊!!!**
在构造任务参数的时候我使用了如下的方式:

```
//通过这种方式初始化的 task 是一个*DoTaskArgs类型
 task := &DoTaskArgs{
	 	JobName:       jobName,
	 	NumOtherPhase: n_other,
	 	Phase:         phase,
	 }
```

后来将构造方式改为如下，测试即可通过：

```
var task DoTaskArgs
	task.JobName = jobName
	task.NumOtherPhase = n_other
	task.Phase = phase
```

其实还有问题，文件0还是没有读取到，但是测试可以通过。以后有再来尝试解决吧，现在还无法解决。