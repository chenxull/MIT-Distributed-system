package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	var keys []string
	var kvs = make(map[string][]string)

	//1.读取 map task 生成的文件,存储到 kvs 中,key同时也存储到 keys 中
	for i := 0; i < nMap; i++ {
		reduceTaskFile := reduceName(jobName, i, reduceTaskNumber)
		fmt.Println("reduceTaskFile name :", reduceTaskFile)
		imm, err := os.Open(reduceTaskFile)
		if err != nil {
			log.Printf("Open reduceTaskFile %s failed.", reduceTaskFile)
			continue
		}
		var kv KeyValue
		dec := json.NewDecoder(imm)
		//读取文件中的数据到 key 中
		err = dec.Decode(&kv)

		//2.重复decode 这些生成的文件，直到返回错误
		for err == nil {
			//如果 key 之前没有出现过，存储到 keys 中，一遍后续的处理。应为在调用reduceF()的时候，需要获取的 key 不能重复
			//fmt.Println("kv中的key 为:", kv.Key)
			if _, ok := kvs[kv.Key]; !ok {
				keys = append(keys, kv.Key)
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
			//decoder and repeatedly calling .Decode(&kv) on it until it returns an error.
			err = dec.Decode(&kv)
		}

	}

	//论文4.2节提出了对 key 进行排序
	sort.Strings(keys)
	out, err := os.Create(outFile)
	if err != nil {
		log.Printf("create output file %s failed.", outFile)
		return
	}

	enc := json.NewEncoder(out)
	//调用reduceF()函数
	for _, key := range keys {
		if err = enc.Encode(KeyValue{key, reduceF(key, kvs[key])}); err != nil {
			log.Printf("write the key: %s to the file %s failed.", key, outFile)
		}
	}
	out.Close()

	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
}
