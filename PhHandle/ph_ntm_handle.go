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

package PhHandle

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/PharbersDeveloper/DL/PhModel"
	"github.com/PharbersDeveloper/DL/PhProxy"
	"github.com/PharbersDeveloper/bp-go-lib/log"
	"net/http"
	"strings"
)

func PhNTMHandle(proxy PhProxy.PhProxy) (handler func(http.ResponseWriter, *http.Request)) {
	bpLog := log.NewLogicLoggerBuilder().Build()

	return func(w http.ResponseWriter, r *http.Request) {
		var response []byte
		var model PhModel.PhModel

		if err := extractModel(r, &model); err != nil || model.IsEmpty() {
			bpLog.Error("Error of the model : " + r.URL.RawQuery)
			response = []byte("Error of the model : " + r.URL.RawQuery)
		} else {
			switch r.Method {
			case "PUT":
				response = []byte("Not Supported")
			case "DELETE":
				response = []byte("Not Supported")
			case "POST":
				response = []byte("Not Supported")
			case "GET":
				tables := strings.Split(model.Model, ",")
				proposalId := model.Query["proposal_id"].(string)
				projectId := model.Query["project_id"].(string)

				pathSlice := strings.Split(r.URL.Path, "/")
				switch strings.ToLower(pathSlice[len(pathSlice)-1]) {
				case "product_ref":
					bpLog.Infof("开始查询产品参考信息表格")
					response, err = productRef(tables, proposalId, projectId, proxy)
				case "product_result":
					bpLog.Infof("开始查询产品结果信息表格")
					response, err = productResult(tables, proposalId, projectId, proxy)
				case "hospital_ref":
					bpLog.Infof("开始查询医院参考信息表格")
					response, err = hospitalRef(tables, proposalId, projectId, proxy)
				default:
					response = []byte("Bad Request URL")
				}

				if err != nil {
					bpLog.Error("Query Error: " + err.Error())
					response = []byte("Query Error: " + err.Error())
				}
			default:
				response = []byte("Bad Request Method")
			}
		}

		w.Header().Set("Access-Control-Allow-Origin", "*")             //允许访问所有域
		w.Header().Add("Access-Control-Allow-Headers", "Content-Type") //header的类型
		w.Header().Set("content-type", "application/json")             //返回数据格式是json
		w.Write(response)
	}
}

func productRef(tables []string, proposalId, projectId string, proxy PhProxy.PhProxy) ([]byte, error) {
	// 获得往期全部聚合信息 ( 往期销售额，往期销售指标 )
	beforeResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Product"},
				[]interface{}{"eq", "product_type", 0},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "phase",
				"aggs": []interface{}{
					map[string]interface{}{
						"groupBy": "product.keyword",
						"aggs": []interface{}{
							map[string]interface{}{
								"agg":   "sum",
								"field": "sales",
							},
							map[string]interface{}{
								"agg":   "sum",
								"field": "quota",
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		return []byte{}, err
	}

	//max, err := findMax(beforeResult, "phase")
	//if err != nil {
	//	return []byte{}, err
	//}

	//maxPhase := max["phase"]
	//println(max)

	return json.Marshal(beforeResult)
}

func productResult(tables []string, proposalId, projectId string, proxy PhProxy.PhProxy) ([]byte, error) {
	return []byte{}, nil
}

func hospitalRef(tables []string, proposalId, projectId string, proxy PhProxy.PhProxy) ([]byte, error) {
	// 获得往期全部聚合信息 ( 往期销售额，往期销售指标 )
	beforeResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Hospital"},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "phase",
				"aggs": []interface{}{
					map[string]interface{}{
						"groupBy": "hospital.keyword",
						"aggs": []interface{}{
							map[string]interface{}{
								"agg":   "sum",
								"field": "sales",
							},
							map[string]interface{}{
								"agg":   "sum",
								"field": "quota",
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		return []byte{}, err
	}

	max, err := findMax(beforeResult, "phase")
	if err != nil {
		return []byte{}, err
	}

	// 获得当期的全部信息 ( 医院名称，代表，患者数量，准入情况，指标贡献率，指标增长率，
	// 指标达成率，销售额同比增长，销售额环比增长，销售额贡献率，YTD销售额 )
	curResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Hospital"},
				[]interface{}{"eq", "phase", max["phase"]},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "hospital.keyword",
				"aggs": []interface{}{
					map[string]interface{}{
						"groupBy": "representative.keyword",
						"aggs": []interface{}{
							map[string]interface{}{
								"agg":   "sum",
								"field": "sales",
							},
							map[string]interface{}{
								"agg":   "sum",
								"field": "quota",
							},
						},
					},
				},
			},
		},
	})

	return json.Marshal(curResult)
}

func compare(prev, next interface{}) (bool, error) {
	switch t := prev.(type) {
	case int:
		return t > next.(int), nil
	case int64:
		return t > next.(int64), nil
	case float32:
		return t > next.(float32), nil
	case float64:
		return t > next.(float64), nil
	case string:
		return t > next.(string), nil
	default:
		return true, errors.New(fmt.Sprintf("%#v, %#v not support compare", prev, next))
	}
}

func findMax(data []map[string]interface{}, key string) (map[string]interface{}, error) {
	tmp := data[0]
	for _, k := range data {
		if b, err := compare(k[key], tmp[key]); err != nil {
			return nil, err
		} else if b {
			tmp = k
		}
	}
	return tmp, nil
}
