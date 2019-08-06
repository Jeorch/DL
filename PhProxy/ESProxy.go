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
	"context"
	"errors"
	"github.com/olivere/elastic/v7"
	"log"
	"os"
)

type ESProxy struct {
	protocol string
	host     string
	port     string
	user     string
	password string
	esClient *elastic.Client
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
	var host = proxy.protocol + "://" + proxy.host + ":" + proxy.port

	errorlog := log.New(os.Stdout, "ES: ", log.LstdFlags)
	client, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetErrorLog(errorlog), elastic.SetURL("http://localhost:9200"))
	if err != nil {
		panic(err)
	}

	info, code, err := client.Ping(host).Do(context.Background())
	if err != nil {
		panic(err)
	}
	log.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)

	esversion, err := client.ElasticsearchVersion(host)
	if err != nil {
		panic(err)
	}
	log.Printf("Elasticsearch version %s\n", esversion)

	proxy.esClient = client
	return &proxy
}

func (proxy ESProxy) Create(table string, insert []map[string]interface{}) (result []map[string]interface{}, err error) {
	bulkRequest := proxy.esClient.Bulk()
	for _, item := range insert {
		req := elastic.NewBulkIndexRequest().Index(table).Doc(item)
		bulkRequest.Add(req)
	}
	bulkResponse, err := bulkRequest.Do(context.Background())

	if err != nil {
		log.Printf("ES插入错误" + err.Error())
		return nil, err
	}
	if bulkResponse.Errors {
		log.Println("ES插入错误")
		return nil, errors.New("ES插入错误")
	}

	for i, item := range bulkResponse.Items {
		tmp := insert[i]
		tmp["_id"] = item["index"].Id
		result = append(result, tmp)
	}
	return
}

// TODO proxy.esClient.UpdateByQuery() 未实现
func (proxy ESProxy) Update(table string, update []map[string]interface{}) (result []map[string]interface{}, err error) {
	return
}

func (proxy ESProxy) Read(table string, query []map[string]interface{}) (result []map[string]interface{}, err error) {
	//table := args.Model
	//if table == "" {
	//	err = errors.New("未指定查询的索引")
	//	return
	//}
	//
	//proxy.esClient.SQL

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

// TODO proxy.esClient.DeleteByQuery() 未实现
func (proxy ESProxy) Delete(table string, query []map[string]interface{}) (result []map[string]interface{}, err error) {
	result = query
	for _, item := range query {
		_, err := proxy.esClient.Delete().Index(table).
			Id(item["_id"].(string)).
			Do(context.Background())
		if err != nil {
			return result, err
		}
	}
	return
}
