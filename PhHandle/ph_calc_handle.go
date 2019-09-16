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
	"errors"
	"fmt"
	"github.com/PharbersDeveloper/DL/PhModel"
	"github.com/PharbersDeveloper/DL/PhProxy"
	"github.com/PharbersDeveloper/bp-go-lib/log"
	"net/http"
	"strings"
)

func PhCalcHandle(proxy PhProxy.PhProxy) (handler func(http.ResponseWriter, *http.Request)) {
	bpLog := log.NewLogicLoggerBuilder().Build()

	parseQuery := func(query map[string]interface{}) (proposalId, projectId string, phase float64, err error) {
		if ok := query["proposal_id"]; ok == nil {
			err = errors.New("Not found `proposal_id` ")
		} else {
			proposalId = ok.(string)
		}

		if ok := query["project_id"]; ok == nil {
			err = errors.New("Not found `project_id` ")
		} else {
			projectId = ok.(string)
		}

		if ok := query["phase"]; ok == nil {
			err = errors.New("Not found `phase` ")
		} else {
			phase = ok.(float64)
		}

		return
	}

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

				pathSlice := strings.Split(r.URL.Path, "/")
				switch strings.ToLower(pathSlice[len(pathSlice)-1]) {
				case "yoy":
					bpLog.Infof("开始查询总产品销售额同比")
					proposalId, projectId, phase, err := parseQuery(model.Query)
					if err != nil {
						response = []byte("Query Error: " + err.Error())
						break
					}

					beforeValue, err := aggProdByPhase(tables, proposalId, projectId, proxy)
					if err != nil {
						response = []byte("Query Error: " + err.Error())
						break
					}

					index, curInfo := findSliceByKeys(beforeValue, map[string]interface{}{
						"phase": phase,
					})
					if index < 0 || curInfo["sum(sales)"] == nil {
						response = []byte("Query Error: " + "查询的周期无数据")
						break
					}
					index, lastYearInfo := findSliceByKeys(beforeValue, map[string]interface{}{
						"phase": phase - 4,
					})
					if index < 0 || lastYearInfo["sum(sales)"] == nil {
						response = []byte("Query Error: " + "查询的周期无去年同期数据")
						break
					}

					value := curInfo["sum(sales)"].(float64) / lastYearInfo["sum(sales)"].(float64) - 1
					response = []byte(fmt.Sprintf("%.4f", value))
				case "mom":
					bpLog.Infof("开始查询总产品销售额环比")
					proposalId, projectId, phase, err := parseQuery(model.Query)
					if err != nil {
						response = []byte("Query Error: " + err.Error())
						break
					}

					beforeValue, err := aggProdByPhase(tables, proposalId, projectId, proxy)
					if err != nil {
						response = []byte("Query Error: " + err.Error())
						break
					}

					index, curInfo := findSliceByKeys(beforeValue, map[string]interface{}{
						"phase": phase,
					})
					if index < 0 || curInfo["sum(sales)"] == nil {
						response = []byte("Query Error: " + "查询的周期无数据")
						break
					}
					index, lastPhaseInfo := findSliceByKeys(beforeValue, map[string]interface{}{
						"phase": phase - 1,
					})
					if index < 0 || lastPhaseInfo["sum(sales)"] == nil {
						response = []byte("Query Error: " + "查询的周期无上期数据")
						break
					}

					value := curInfo["sum(sales)"].(float64) / lastPhaseInfo["sum(sales)"].(float64) - 1
					response = []byte(fmt.Sprintf("%.4f", value))
				default:
					response = []byte("Bad Request URL")
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

func aggProdByPhase(tables []string, proposalId, projectId string, proxy PhProxy.PhProxy) (result []map[string]interface{}, err error) {
	// 聚合往期产品的销售和 ( 产品名称，sum销售额 )
	result, err = proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Hospital"},
				[]interface{}{"neq", "product_type", 1},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "phase",
				"aggs": []interface{}{
					map[string]interface{}{
						"agg":   "sum",
						"field": "sales",
					},
				},
			},
		},
	})
	return
}
