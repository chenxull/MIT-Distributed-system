package mapreduce

import (
	"fmt"
	"log"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		//ntasks 每个阶段的任务数量
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	//1.调度器等待所有的工作完成
	var wg sync.WaitGroup

	//2.构造任务参数
	// task := &DoTaskArgs{
	// 	JobName:       jobName,
	// 	NumOtherPhase: n_other,
	// 	Phase:         phase,
	// }
	var task DoTaskArgs
	task.JobName = jobName
	task.NumOtherPhase = n_other
	task.Phase = phase

	//3.通过 channel 的方式将任务ID分配出去，总任务数为ntasks。并且等待任务的完成
	taskChan := make(chan int)

	go func() {
		for i := 0; i < ntasks; i++ {
			//在分配任务ID之前，等待+1.当 wg 的计数器不为0时，会一直在wg.wait处阻塞
			wg.Add(1)
			fmt.Println("DEBUG::分配任务ID", i)
			taskChan <- i
		}
		wg.Wait()
		close(taskChan)
	}()

	//3.给 worker 分配任务
	for i := range taskChan {
		worker := <-registerChan

		task.TaskNumber = i
		fmt.Println("debug::任务号", i)
		//task.File 只在 map 阶段使用。
		if phase == mapPhase {
			task.File = mapFiles[i]

		}
		go func(worker string, task DoTaskArgs) {
			if call(worker, "Worker.DoTask", &task, nil) {
				//成功完成工作后，释放等待
				wg.Done()
				//空闲状态信息发给registerChan
				registerChan <- worker
			} else {
				log.Printf("Schedule : assigned %s task %d to %s worker failed.",
					phase, task.TaskNumber, worker)
				//将任务 id 放入的 taskChan中，等待重新分配 worker去执行
				taskChan <- task.TaskNumber
			}
		}(worker, task)

	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
