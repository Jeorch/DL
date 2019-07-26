package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/PharbersDeveloper/DL/PhHandle"
	"github.com/PharbersDeveloper/DL/PhProxy"
)

var ip = "192.168.100.157"
var port = "9001"
var ESHost = "192.168.100.174"
var ESPort = "9200"
var WriteTimeout = time.Second * 4

func main() {
	proxy := PhProxy.ESProxy{}.NewProxy(map[string]string{
		"host": ESHost,
		"port": ESPort,
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/", PhHandle.PhHandle(proxy))

	// 主动关闭服务器
	var server *http.Server

	// 一个通知退出的chan
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)

	server = &http.Server{
		Addr:         ip + ":" + port,
		WriteTimeout: WriteTimeout,
		Handler:      mux,
	}

	log.Println("Starting httpserver")

	go func() {
		// 接收退出信号
		<-quit
		if err := server.Close(); err != nil {
			log.Fatal("Close server:", err)
		}
	}()

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
