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
	"github.com/PharbersDeveloper/DL/PhHandle"
	"github.com/PharbersDeveloper/DL/PhProxy"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
)

var ip = ""
var port = "9201"
var WriteTimeout = time.Second * 4
var ESHost = "192.168.100.174"
var ESPort = "9200"

func main() {
	if ok := os.Getenv("DL_PORT"); ok != "" {
		port = ok
	}
	if ok := os.Getenv("ES_HOST"); ok != "" {
		ESHost = ok
	}
	if ok := os.Getenv("ES_PORT"); ok != "" {
		ESPort = ok
	}

	addr := ip + ":" + port
	proxy := PhProxy.ESProxy{}.NewProxy(map[string]string{
		"host": ESHost,
		"port": ESPort,
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/v1.0/DL", PhHandle.PhHandle(proxy))

	/// 下面不用管，网上抄的
	// 主动关闭服务器
	var server *http.Server

	// 一个通知退出的chan
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)

	server = &http.Server{
		Addr:         addr,
		WriteTimeout: WriteTimeout,
		Handler:      mux,
	}

	log.Println("Starting httpserver in " + port)

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
