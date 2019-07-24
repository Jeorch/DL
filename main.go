package main

import (
	"net/http"
	"log"
	"io/ioutil"
	"fmt"
	"os"
	"os/signal"
	"time"
	"encoding/json"
)

var port = "9000"
// 主动关闭服务器
var server *http.Server

func main() {
	// 一个通知退出的chan
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)

	mux := http.NewServeMux()
	mux.HandleFunc("/data", forwardES)

	server = &http.Server{
		Addr:         ":" + port,
		WriteTimeout: time.Second * 4,
		Handler:      mux,
	}

	go func() {
		// 接收退出信号
		<-quit
		if err := server.Close(); err != nil {
			log.Fatal("Close server:", err)
		}
	}()

	log.Println("Starting httpserver")

	err := server.ListenAndServe()
	if err != nil {
		// 正常退出
		if err == http.ErrServerClosed {
			log.Fatal("Server closed under request")
		} else {
			log.Fatal("Server closed unexpected", err)
		}
	}
	log.Fatal("Server exited")
}

func forwardES(w http.ResponseWriter, _ *http.Request) {
	title := make([]interface{}, 0)
	title = append(title, "firstname")
	title = append(title, "age")

	response, err := http.Get("http://127.0.0.1:9200/index/_doc/_search?_source=firstname,age")
	if err != nil {
		// handle error
	}
	//程序在使用完回复后必须关闭回复的主体。
	defer response.Body.Close()

	body, _ := ioutil.ReadAll(response.Body)
	var dat map[string]interface{}
	json.Unmarshal(body, &dat)

	root := make([][]interface{}, 0)
	root = append(root, title)

	if v, ok := dat["hits"]; ok {
		if hits, ok := v.(map[string]interface{})["hits"]; ok {
			items := hits.([]interface{})
			for _, item := range items {
				arr := make([]interface{}, 0)
				if source, ok := item.(map[string]interface{})["_source"]; ok {
					obj := source.(map[string]interface{})
					for _, k := range title {
						arr = append(arr, obj[k.(string)])
					}
				}
				root = append(root, arr)
			}
		}
	}
	fmt.Println(root)

	byteSlice, err := json.Marshal(root)

	w.Header().Set("Access-Control-Allow-Origin", "*")             //允许访问所有域
	w.Header().Add("Access-Control-Allow-Headers", "Content-Type") //header的类型
	w.Header().Set("content-type", "application/json")             //返回数据格式是json
	w.Write(byteSlice)
}
