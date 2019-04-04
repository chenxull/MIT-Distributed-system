# MIT 的分布式系统课程
计划用2个月的时间来学一下这门课程

##Lab1 mapreduce part1


- 在完成了 doMap 和 doReduce 的功能之后，测试发现，最后输出文件中的内容为空，导致测试失败。最开始定位问题以为子 doReduce 函数中。后来进过不断的 debug 是 doMap函数中，最后存储读取到的文件时发生错误。应该将整个 kv 信息存储到输出文件中供doReduce函数使用，我只存储了 kv.value信息, doReduce函数在最后遍历 keys 无法获取到 key，最后导致结果文件中的数据为空。