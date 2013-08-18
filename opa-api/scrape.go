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
	killCode   = -666
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
func get(n int, feed chan int, save chan *Control) {
	fmt.Println("starting get: ", n)
	for {
		select {
		case id := <-feed:
			if id == killCode {
				save <- &Control{op: "kill"}
				return
			}
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
			save <- &Control{op: "save", resource: &Resource{Id: id, Response: r}}
		}
	}
}

func save(n int, ch chan *Control, csv chan *Resource, wg *sync.WaitGroup) {
	for {
		select {
		case control := <-ch:
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

	//read, err := ioutil.ReadFile(progressFilename)
	//if err != nil {
	//		fmt.Println("No progress file found, getting all ids")
	//	}

	scrape(f)
}

func scrape(ids Ids) {
	c := *concurrent

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

	var wg sync.WaitGroup
	csvChan := make(chan *Resource, 1000)
	go writeRow(csvChan, csvo, csvv, &wg)

	feedChan := make(chan int)

	for i := 0; i < c; i++ {
		ch := make(chan *Control, 100)

		// Wait until each channel has been killed
		wg.Add(1)

		go get(i, feedChan, ch)
		go save(i, ch, csvChan, &wg)

	}

	for _, id := range ids {
		feedChan <- id
	}

	// Kill all channels when they are done with ids
	n := make([]int, c)
	for _, _ = range n {
		feedChan <- killCode
	}
	csvChan <- &Resource{Id: killCode}

	wg.Wait()
}
