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
package PhProxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	. "github.com/PharbersDeveloper/DL/PhModel"
	. "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/estransport"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type ESProxy struct {
	protocol string
	host     string
	port     string
	user     string
	password string
	esClient *Client
}

func (proxy ESProxy) NewProxy(args map[string]string) *ESProxy {
	protocol := args["protocol"]
	if "" == protocol {
		protocol = "http"
	}

	proxy.protocol = protocol
	proxy.host = args["host"]
	proxy.port = args["port"]
	proxy.user = args["user"]
	proxy.password = args["pwd"]

	return proxy.connectES()
}

func (proxy ESProxy) connectES() *ESProxy {
	cfg := Config{
		Addresses: []string{
			proxy.protocol + "://" + proxy.host + ":" + proxy.port,
		},
		Username: proxy.user,
		Password: proxy.password,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: 10 * time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS11,
			},
		},
		Logger: &estransport.ColorLogger{
			Output: os.Stdout,
			EnableRequestBody:  true,
			EnableResponseBody: true,
		},
	}
	es, err := NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	proxy.esClient = es
	return &proxy
}

func (proxy ESProxy) Create(args PhModel) (err error) {
	model := args.Model
	data := args.Insert
	if model == "" {
		return errors.New("未指定插入的索引")
	}
	if data == nil {
		return errors.New("插入的数据为空")
	}

	var wg sync.WaitGroup
	for i, title := range data {
		wg.Add(1)

		// 启动协程并发写入
		go func(i int, row map[string]interface{}) {
			defer wg.Done()

			jsonBytes, err := json.Marshal(row)
			// Set up the request object.
			req := esapi.IndexRequest{
				Index: model,
				//DocumentID: strconv.Itoa(i + 1), // 自动生成id
				Body:    strings.NewReader(string(jsonBytes)),
				Refresh: "true",
			}

			// Perform the request with the client.
			res, err := req.Do(context.Background(), proxy.esClient)
			if err != nil {
				log.Fatalf("Error getting response: %s", err)
			}
			defer res.Body.Close()

			if res.IsError() {
				log.Printf("[%s] Error indexing document, obj = %s", res.Status(), string(jsonBytes))
			}
		}(i, title)
	}
	wg.Wait()

	return
}

func (proxy ESProxy) Update(args PhModel) (data map[string]interface{}, err error) {
	return
}

func (proxy ESProxy) Read(args PhModel) (data []map[string]interface{}, err error) {
	//proxy.esClient.
	//reqMethod := "GET"
	//body := args
	//
	//reqUrl := fmt.Sprintf("%s://%s:%s/%s/_doc/_search",
	//	proxy.Protocol,
	//	proxy.Host,
	//	proxy.Port,
	//	args.Model,
	//)

	//result, err := callHttp(reqMethod, reqUrl, body)
	//data, err = format(result)
	return
}

func (proxy ESProxy) Delete(args PhModel) (data map[string]interface{}, err error) {
	return
}

func callHttp(method, url string, body interface{}) (data map[string]interface{}, err error) {
	var bodyReader io.Reader
	if body != nil {
		bodyBytes, _ := json.Marshal(body)
		bodyReader = bytes.NewReader(bodyBytes)
	}

	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return
	}

	req.Header.Add("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}

	respBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}
	json.Unmarshal(respBody, &data)

	return
}
