package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

const maxCount = 6

func ExecutePipeline(workers ...job) {
	wg := &sync.WaitGroup{}
	var in chan interface{}
	out := make(chan interface{})
	for _, worker := range workers {
		wg.Add(1)

		go func(in, out chan interface{}, wg *sync.WaitGroup, worker job) {
			defer wg.Done()
			defer close(out)
			worker(in, out)
		}(in, out, wg, worker)

		in = out
		out = make(chan interface{})
	}

	wg.Wait()
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mtx := &sync.Mutex{}
	for element := range in {
		wg.Add(1)

		go func(data string, wg *sync.WaitGroup, out chan interface{}, mtx *sync.Mutex) {
			defer wg.Done()

			chanMd5 := make(chan string)
			go func(chanMd5 chan string, data string, mtx *sync.Mutex) {
				mtx.Lock()
				chanMd5 <- DataSignerMd5(data)
				mtx.Unlock()
			}(chanMd5, data, mtx)

			chanCrc32 := make(chan string)
			go func(chanCrc32 chan string, data string) {
				chanCrc32 <- DataSignerCrc32(data)
			}(chanCrc32, data)

			chanCrc32Md5 := make(chan string)
			go func(chanCrc32Md5 chan string, data string) {
				chanCrc32Md5 <- DataSignerCrc32(data)
			}(chanCrc32Md5, <-chanMd5)

			out <- <-chanCrc32 + "~" + <-chanCrc32Md5
		}(strconv.Itoa(element.(int)), wg, out, mtx)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	wgAll := &sync.WaitGroup{}
	for element := range in {
		wgAll.Add(1)

		go func(element interface{}, wgAll *sync.WaitGroup) {
			defer wgAll.Done()
			wg := &sync.WaitGroup{}
			data := make([]string, maxCount)
			for i := 0; i < maxCount; i++ {
				wg.Add(1)

				go func(data []string, str string, idx int, wg *sync.WaitGroup) {
					defer wg.Done()
					data[idx] = DataSignerCrc32(str)
				}(data, strconv.Itoa(i)+element.(string), i, wg)
			}
			wg.Wait()
			out <- strings.Join(data, "")
		}(element, wgAll)
	}
	wgAll.Wait()
}

func CombineResults(in, out chan interface{}) {
	var res []string
	for element := range in {
		res = append(res, element.(string))
	}
	sort.Strings(res)
	out <- strings.Join(res, "_")
}
