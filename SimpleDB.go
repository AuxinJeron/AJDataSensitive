package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

const dbFile = "./output/db.data"

func main() {
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
	f, err := os.Open(dbFile)
	check(err)
	defer f.Close()
	reader := bufio.NewReader(f)
	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			break
		}
		elem = string(line)
	}
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

	bytes, err := f.WriteString(fmt.Sprintf("%s,%s\n", key, value))
	check(err)
	fmt.Printf("Wrote %d bytes\n", bytes)
	f.Sync()
}
