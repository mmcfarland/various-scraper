package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
)

var (
	concurrent = flag.Int("c", 1, "Number of concurrent requests allowed")
	filename   = flag.String("f", "", "Source json file of OPA ids")
	saveDir, _ = ioutil.TempDir(".", "opa-downloads")
)

var serviceUrl = "http://services.phila.gov/OPA/v1.0/account"

type Resource struct {
	Id       int
	Response string
}

type Control struct {
	op       string // "save", "kill"
	resource *Resource
}

type Ids []int

// Request and receive opa responses based on a list of account ids
func get(n int, c chan *Control, ids []int) {
	fmt.Println("starting get: ", n, len(ids))
	for _, id := range ids {
		url := fmt.Sprintf("%s/%v?format=json", serviceUrl, id)
		resp, err := http.Get(url)
		if err != nil {
			fmt.Println("get err", err)
			continue
		}
		defer resp.Body.Close()
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("read err", err)
			continue
		}
		r := string(b)
		c <- &Control{op: "save", resource: &Resource{Id: id, Response: r}}
	}
	// Terminate the routine
	c <- &Control{op: "kill"}
}

func save(n int, cr chan *Control, csv chan *Resource, wg *sync.WaitGroup) {
	for {
		select {
		case control := <-cr:
			if control.op == "kill" {
				wg.Done()
				return
			}
			fn := path.Join(saveDir, strconv.Itoa(control.resource.Id)+".json")
			f, err := os.Create(fn)
			if err != nil {
				fmt.Println(err)
			}
			_, err = f.Write([]byte(control.resource.Response))
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("wrote to ", f.Name())
			f.Close()

			csv <- control.resource
		}
	}
}

func writeRow(c chan *Resource, w *csv.Writer) {
	for {
		select {
		case res := <-c:
			var f interface{}
			err := json.Unmarshal([]byte(res.Response), &f)
			if err != nil {
				fmt.Println(err)
			}
			j := f.(map[string]interface{})
			if j["status"] != "success" {
				fmt.Println(res.Response)
			}
			r := make([]string, 5)
			i := 0
			d := j["data"].(map[string]interface{})
			p := d["property"].(map[string]interface{})
			for _, v := range p {
				switch v.(type) {
				case string:
					r[i] = v.(string)
					i++
				case map[string]interface{}:
					//fmt.Println(k + " is node")
				}
			}
			fmt.Println(r)
			err = w.Write(r)
			if err != nil {
				fmt.Println(err)
			}
			w.Flush()
		}
	}
}

func main() {
	flag.Parse()
	var f Ids
	b, err := ioutil.ReadFile(*filename)
	if err != nil {
		fmt.Println("Couldn't read file", err)
		return
	}
	err = json.Unmarshal(b, &f)
	scrape(f)
}

func scrape(ids Ids) {
	c := *concurrent
	cnt := cap(ids)
	start := 0
	span := cnt / c
	end := span

	// Setup a csv output for each download
	f, err := os.Create("opa.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	csv := csv.NewWriter(w)
	csvChan := make(chan *Resource, 100)
	go writeRow(csvChan, csv)

	var wg sync.WaitGroup
	for i := 0; i < c; i++ {
		ch := make(chan *Control, 10)

		// Sanitize the parts of the slice for each goroutine
		if start > cnt-1 {
			break
		}
		if end >= cnt {
			end = cnt - 1
		}
		if c == i+1 && end <= cnt {
			end = cnt
		}

		wg.Add(1)
		// Download and save resources
		fmt.Println(start, "-", end)
		go get(i, ch, ids[start:end])
		go save(i, ch, csvChan, &wg)

		start = end
		end = end + span
	}
	wg.Wait()
	csv.Flush()
}
