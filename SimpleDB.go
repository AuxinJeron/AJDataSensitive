package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

const dbFile = "./output/db.data"
const indexFile = "./output/index.data"

var indexMap map[string]int64

func main() {
	load_index()

	args := os.Args[1:]
	operation := args[0]
	key := args[1]
	var value string
	if len(args) >= 3 {
		value = args[2]
	}

	if operation == "get" {
		fmt.Printf("Get value for key '%s' is '%s'\n", key, db_get(key))
	} else if operation == "set" {
		db_set(key, value)
		fmt.Printf("Set value '%s' for key '%s'\n", value, key)
	}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func db_get(key string) string {
	var elem string
	var result string
	var pos int64

	// Query the index
	if val, ok := indexMap[key]; ok {
		pos = val
	}

	f, err := os.Open(dbFile)
	check(err)
	defer f.Close()

	reader := bufio.NewReader(f)
	_, err = reader.Discard(int(pos))
	check(err)

	line, _, err := reader.ReadLine()
	check(err)
	elem = string(line)

	if elem != "" {
		split := strings.Split(elem, ",")
		result = split[1]
	}
	return result
}

func db_set(key string, value string) {
	f, err := os.OpenFile(dbFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	check(err)
	defer f.Close()

	fileInfo, err := os.Stat(dbFile)
	check(err)
	pos := fileInfo.Size()

	_, err = f.WriteString(fmt.Sprintf("%s,%s\n", key, value))
	check(err)
	f.Sync()

	fmt.Printf("Wrote index {'%s','%d'}\n", key, pos)
	indexMap[key] = pos
	save_index()
}

func load_index() map[string]int64 {
	dat, err := ioutil.ReadFile(indexFile)

	if err != nil {
		// Return an empty index map if there is no index map
		indexMap = make(map[string]int64)

		return make(map[string]int64)
	}

	// Decode the loaded index map
	buffer := bytes.NewBuffer(dat)
	decoder := gob.NewDecoder(buffer)
	err = decoder.Decode(&indexMap)

	fmt.Println("Loaded index:", indexMap)
	return indexMap
}

func save_index() {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(indexMap)
	check(err)

	f, err := os.OpenFile(indexFile, os.O_WRONLY|os.O_CREATE, 0755)
	check(err)
	defer f.Close()

	_, err = f.Write(buffer.Bytes())
	check(err)
}
