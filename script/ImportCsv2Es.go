/*
 * This file is part of com.pharbers.DL.
 *
 * com.pharbers.DL is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * com.pharbers.DL is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <https://www.gnu.org/licenses/>.
 */
package main

import (
	"bytes"
	. "github.com/PharbersDeveloper/DL/PhModel"
	. "github.com/PharbersDeveloper/DL/PhProxy"
	. "github.com/recursionpharma/go-csv-map"
	"io/ioutil"
	"log"
	"strconv"
)

func main() {
	var date = "2018Q1"

	fileName := "/Users/clock/Downloads/TMResult.csv"
	dataBytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		panic(err.Error())
	}

	reader := NewReader(bytes.NewBufferString(string(dataBytes)))
	reader.Columns, err = reader.ReadHeader()
	if err != nil {
		panic(err.Error())
	}

	records, err := reader.ReadAll()
	if err != nil {
		panic(err.Error())
	}

	sliceMapInterface := make([]map[string]interface{}, 0)
	for i := 0; i < len(records); i++ {
		records[i]["date"] = date
		mapInterface := make(map[string]interface{})
		for k, v := range records[i] {
			mapInterface[k] = convertString(v)
		}
		sliceMapInterface = append(sliceMapInterface, mapInterface)
	}

	err = InsertES(sliceMapInterface)
	if err != nil {
		panic(err.Error())
	}

	log.Println("导入完成")
}

func convertString(str string) interface{} {
	if i, err := strconv.Atoi(str); err == nil {
		return i
	}
	if l, err := strconv.ParseInt(str, 10, 64); err == nil {
		return l
	}
	if b, err := strconv.ParseBool(str); err == nil {
		return b
	}
	if f, err := strconv.ParseFloat(str, 32); err == nil {
		return f
	}
	if d, err := strconv.ParseFloat(str, 64); err == nil {
		return d
	}
	return str
}

func InsertES(data []map[string]interface{}) error {
	var tESHost = "192.168.100.157"
	var tESPort = "9200"
	var importIndex = "oldtm"

	proxy := ESProxy{}.NewProxy(map[string]string{
		"host": tESHost,
		"port": tESPort,
	})

	model := PhModel {
		Model: importIndex,
		Insert: data,
	}

	return proxy.Create(model)
}
