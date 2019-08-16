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
	"encoding/json"
	"errors"
	"github.com/olivere/elastic/v7"
	"log"
	"os"
	"reflect"
	"strings"
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
	client, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetErrorLog(errorlog), elastic.SetURL(host))
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
	log.Println("ES Connect To: " + host)

	proxy.esClient = client
	return &proxy
}

func (proxy ESProxy) Create(table string, insert []map[string]interface{}) (result []map[string]interface{}, err error) {
	bulkRequest := proxy.esClient.Bulk()
	for _, item := range insert {
		req := elastic.NewBulkIndexRequest().Index(table).Doc(item)
		bulkRequest = bulkRequest.Add(req)
	}
	bulkResponse, err := bulkRequest.Do(context.Background())

	if err != nil {
		log.Printf("ES插入错误" + err.Error())
		return nil, err
	}
	if bulkResponse.Errors {
		errMsg := "ES插入错误" + bulkResponse.Items[0]["index"].Error.Reason
		return nil, errors.New(errMsg)
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

func (proxy ESProxy) Read(table []string, query map[string]interface{}) (result []map[string]interface{}, err error) {
	searchService := proxy.esClient.Search(table...)

	// search.size /search.sort / search.or / search.and
	esCondUtil{ser: searchService}.
		genQueryCond(query["search"]).
		genAggCond(query["aggs"])

	res, err := searchService.Do(context.Background())
	if err != nil {
		return
	}
	if res.Error != nil {
		err = errors.New(res.Error.Reason)
	}

	if res.Aggregations != nil {
		result = esGetResultUtil{}.getAggResult(res)
	} else {
		result = esGetResultUtil{}.getSearchResult(res)
	}

	return
}

// TODO proxy.esClient.DeleteByQuery() 未实现
func (proxy ESProxy) Delete(table string, query []map[string]interface{}) (result []map[string]interface{}, err error) {
	//result = query
	//for _, item := range query {
	//	_, err := proxy.esClient.Delete().Index(table).
	//		Id(item["_id"].(string)).
	//		Do(context.Background())
	//	if err != nil {
	//		return result, err
	//	}
	//}
	return
}

type esCondUtil struct{ ser *elastic.SearchService }

func (util esCondUtil) genBaseQuery(oper []interface{}) elastic.Query {
	var query elastic.Query
	switch oper[0].(string) {
	case "eq":
		query = elastic.NewMatchQuery(oper[1].(string), oper[2])
	case "neq":
		query := elastic.NewBoolQuery()
		query.MustNot(elastic.NewMatchQuery(oper[1].(string), oper[2]))
	case "gt":
		query = elastic.NewRangeQuery(oper[1].(string)).Gt(oper[2])
	case "gte":
		query = elastic.NewRangeQuery(oper[1].(string)).Gte(oper[2])
	case "lt":
		query = elastic.NewRangeQuery(oper[1].(string)).Lt(oper[2])
	case "lte":
		query = elastic.NewRangeQuery(oper[1].(string)).Lte(oper[2])
	default:
		log.Println("不支持的查询函数" + oper[0].(string))
	}
	return query
}

func (util esCondUtil) genBoolQuery(oper string, subOpers interface{}) elastic.Query {
	// 先解析包裹的子查询
	queries := make([]elastic.Query, 0)
	for _, oper := range subOpers.([]interface{}) {
		oper := oper.([]interface{})
		switch oper[0].(string) {
		case "or":
			queries = append(queries, util.genBoolQuery("or", oper[1]))
		case "and":
			queries = append(queries, util.genBoolQuery("and", oper[1]))
		default:
			queries = append(queries, util.genBaseQuery(oper))
		}
	}

	// 再解析本次操作符
	query := elastic.NewBoolQuery()
	switch oper {
	case "or":
		query.Should(queries...)
	case "and":
		query.Must(queries...)
	default:
		log.Println("不支持的查询函数" + oper)
	}

	return query
}

func (util esCondUtil) genQueryCond(search interface{}) esCondUtil {
	if search == nil {
		util.ser.Query(nil)
		return util
	}

	for k, v := range search.(map[string]interface{}) {
		switch k {
		case "from":
			util.ser.From(int(v.(float64)))
		case "size":
			util.ser.Size(int(v.(float64)))
		case "sort":
			for _, str := range v.([]interface{}) {
				if strings.HasPrefix(str.(string), "-") {
					util.ser.Sort(str.(string)[1:], false)
				} else if strings.HasPrefix(str.(string), "+") {
					util.ser.Sort(str.(string)[1:], true)
				} else {
					util.ser.Sort(str.(string), true)
				}
			}
		case "and", "or":
			util.ser.Query(util.genBoolQuery(k, v))
		}
	}

	return util
}

func (util esCondUtil) genBaseAgg(oper, field string) (string, elastic.Aggregation) {
	var agg elastic.Aggregation
	switch oper {
	case "max":
		agg = elastic.NewMaxAggregation().Field(field)
	case "min":
		agg = elastic.NewMinAggregation().Field(field)
	case "avg":
		agg = elastic.NewAvgAggregation().Field(field)
	case "sum":
		agg = elastic.NewSumAggregation().Field(field)
	default:
		log.Println("不支持的聚合函数" + oper)
	}
	return oper + "(" + field + ")", agg
}

func (util esCondUtil) genRecAgg(aggregation map[string]interface{}) (field string, result elastic.Aggregation) {
	field = aggregation["groupBy"].(string)
	aggs := aggregation["aggs"].([]interface{})

	terms := elastic.NewTermsAggregation()
	if strings.HasPrefix(field, "-") {
		field = field[1:]
		terms.Field(field).OrderByKey(false)
	} else if strings.HasPrefix(field, "+") {
		field = field[1:]
		terms.Field(field).OrderByKey(true)
	} else {
		terms.Field(field)
	}

	for _, agg := range aggs {
		agg := agg.(map[string]interface{})

		if _, ok := agg["groupBy"]; ok {
			terms.SubAggregation(util.genRecAgg(agg))
		}

		if oper, ok := agg["agg"]; ok {
			oper := oper.(string)
			field := agg["field"].(string)

			sort := ""
			if strings.HasPrefix(field, "-") {
				sort = "desc"
				field = field[1:]
			} else if strings.HasPrefix(field, "+") {
				sort = "asc"
				field = field[1:]
			}

			if name, sub := util.genBaseAgg(oper, field); sub != nil {
				terms.SubAggregation(name, sub)
				switch sort {
				case "desc":
					terms.SubAggregation("sort("+name+")", elastic.NewBucketSortAggregation().Sort(name, false))
				case "asc":
					terms.SubAggregation("sort("+name+")", elastic.NewBucketSortAggregation().Sort(name, true))
				default:
				}
			}
		}
	}

	return field, terms
}

func (util esCondUtil) genAggCond(aggs interface{}) esCondUtil {
	if aggs == nil {
		return util
	}

	for _, item := range aggs.([]interface{}) {
		util.ser.Aggregation(util.genRecAgg(item.(map[string]interface{})))
	}

	return util
}

type esGetResultUtil struct{}

// 从搜索结果中取数据
func (util esGetResultUtil) getSearchResult(res *elastic.SearchResult) (result []map[string]interface{}) {
	type typ map[string]interface{}
	var tmp typ
	for _, item := range res.Each(reflect.TypeOf(tmp)) {
		result = append(result, item.(typ))
	}
	return result
}

// 从聚合结果中取数据
func (util esGetResultUtil) getAggResult(res *elastic.SearchResult) []map[string]interface{} {
	// 第一步，map[string, RawMessage] => map[string, map[string, interface{}]
	aggs := make(map[string]interface{})
	for k, v := range res.Aggregations {
		tmpRawMessage := make(map[string]interface{})
		bs, _ := v.MarshalJSON()
		json.Unmarshal(bs, &tmpRawMessage)
		aggs[k] = tmpRawMessage
	}

	// 第二步，ES 返回结构 转为 []{map[key1, key2, agg1, agg2]}
	return util.getAggRec(aggs)
}

// ES 返回结构 转为 []{map[key1, key2, agg1, agg2]}
func (util esGetResultUtil) getAggRec(data map[string]interface{}) (result []map[string]interface{}) {
	lastMap := make(map[string]interface{})

	for aggKey, aggValue := range data {
		valueMap := aggValue.(map[string]interface{})
		if buckets, ok := valueMap["buckets"]; ok {
			for _, item := range buckets.([]interface{}) {
				bucket := item.(map[string]interface{})
				key := bucket["key"]
				delete(bucket, "key")
				delete(bucket, "doc_count")
				for _, sub := range util.getAggRec(bucket) {
					sub[aggKey] = key
					result = append(result, sub)
				}
			}
		} else {
			lastMap[aggKey] = valueMap["value"]
		}
	}
	if len(lastMap) != 0 {
		result = append(result, lastMap)
	}
	return result
}
