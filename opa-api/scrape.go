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
			f.Close()

			csv <- control.resource
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

	// Setup a csv output for the opa main and
	// valuation (1-N) records
	f, err := os.Create("opa.csv")
	if err != nil {
		panic(err)
	}
	fv, err := os.Create("opa-val.csv")
	if err != nil {
		panic(err)
	}

	defer f.Close()
	defer fv.Close()

	w := bufio.NewWriter(f)
	wv := bufio.NewWriter(fv)
	csvo := csv.NewWriter(w)
	csvv := csv.NewWriter(wv)

	defer csvo.Flush()
	defer csvv.Flush()

	csvChan := make(chan *Resource, 100)
	go writeRow(csvChan, csvo, csvv)

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
}
