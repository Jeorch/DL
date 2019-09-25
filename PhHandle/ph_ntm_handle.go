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
	"os"
	"strconv"
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

				pathSlice := strings.Split(r.URL.Path, "/")
				switch strings.ToLower(pathSlice[len(pathSlice)-1]) {
				case "product_ref":
					bpLog.Infof("开始查询产品信息表格")
					response, err = productRef(tables, model.Query, proxy)
				case "rep_ref":
					bpLog.Infof("开始查询代表信息表格")
					response, err = repRef(tables, model.Query, proxy)
				case "rep_prod":
					bpLog.Infof("开始查询代表下产品信息表格")
					response, err = repProd(tables, model.Query, proxy)
				case "hospital_ref":
					bpLog.Infof("开始查询医院信息表格")
					response, err = hospitalRef(tables, model.Query, proxy)
				case "hospital_prod":
					bpLog.Infof("开始查询医院下产品信息表格")
					response, err = hospitalProd(tables, model.Query, proxy)
				case "region_ref":
					bpLog.Infof("开始查询区域信息表格")
					response, err = regionRef(tables, model.Query, proxy)
				case "region_prod":
					bpLog.Infof("开始查询区域下产品信息表格")
					response, err = regionProd(tables, model.Query, proxy)
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

func productRef(tables []string, query map[string]interface{}, proxy PhProxy.PhProxy) ([]byte, error) {
	proposalId, projectId, pointOrigin, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	// 获得往期产品的全部信息 ( 产品名称，其他 )
	beforeResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Product"},
				[]interface{}{"neq", "product_type", 1},
			},
		},
	})
	if err != nil {
		return []byte{}, err
	}

	// 转换为各个周期的销售额透视图
	phaseSalesPivot := genPivot(beforeResult,
		func(item map[string]interface{}) string { return item["product"].(string) },
		"phase", "sales",
	)

	// 转换为各个周期的销售指标透视图
	phaseQuotaPivot := genPivot(beforeResult,
		func(item map[string]interface{}) string { return item["product"].(string) },
		"phase", "quota",
	)

	// 取得最新的周期的产品信息
	curInfo, err := findMaxByKey(beforeResult, "phase")
	if err != nil {
		return []byte{}, err
	}

	// 计算YTD的销售额
	maxPhase := int(curInfo[0]["phase"].(float64))
	minPhase, err := findSameYear(maxPhase, pointOrigin)
	if err != nil {
		return []byte{}, err
	}
	ytdResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Product"},
				[]interface{}{"gte", "phase", minPhase},
				[]interface{}{"lte", "phase", maxPhase},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "product.keyword",
				"aggs": []interface{}{
					map[string]interface{}{
						"agg":   "sum",
						"field": "sales",
					},
				},
			},
		},
	})
	if err != nil {
		return []byte{}, err
	}

	// 计算最新的周期指标和销售的总和
	curTotalQuota := 0.0
	curTotalSales := 0.0
	for _, info := range curInfo {
		curTotalQuota += info["quota"].(float64)
		curTotalSales += info["sales"].(float64)
	}

	// ( 产品名称，指标贡献率，指标增长率，
	// 指标达成率，销售额同比增长，销售额环比增长，销售额贡献率，YTD销售额 ) + pivot sales by phase
	var curResult = make([]map[string]interface{}, 0)
	for _, info := range curInfo {
		var tmp = make(map[string]interface{}, 0)

		tmp["product"] = info["product"]

		tmp["quota_contri"] = calcContri(info["quota"], curTotalQuota)
		_, lastPhaseInfo := findSliceByKeys(beforeResult, map[string]interface{}{
			"product": info["product"],
			"phase":   info["phase"].(float64) - 1,
		})
		tmp["quota_growth"] = calcGrowth(info["quota"], lastPhaseInfo["sales"])
		tmp["quota_rate"] = calcAchieving(info["sales"], info["quota"])

		_, lastYearInfo := findSliceByKeys(beforeResult, map[string]interface{}{
			"product": info["product"],
			"phase":   info["phase"].(float64) - 4,
		})
		if lastYearInfo == nil {
			tmp["year_on_year_sales"] = 0.0
		} else {
			tmp["year_on_year_sales"] = calcGrowth(info["sales"], lastYearInfo["sales"])
		}
		tmp["sales_growth"] = calcGrowth(info["sales"], lastPhaseInfo["sales"])
		tmp["sales_contri"] = calcContri(info["sales"], curTotalSales)

		_, ytdInfo := findSliceByKeys(ytdResult, map[string]interface{}{
			"product.keyword": info["product"],
		})
		if ytdInfo == nil {
			tmp["ytd_sales"] = 0.0
		} else {
			tmp["ytd_sales"] = ytdInfo["sum(sales)"]
		}

		pivotSales := phaseSalesPivot[info["product"].(string)]
		for k, v := range pivotSales {
			tmp["sales_"+fmt.Sprintf("%d", int(k.(float64)))] = v
		}
		pivotQuota := phaseQuotaPivot[info["product"].(string)]
		for k, v := range pivotQuota {
			tmp["quota_"+fmt.Sprintf("%d", int(k.(float64)))] = v
		}

		curResult = append(curResult, tmp)
	}

	return json.Marshal(curResult)
}

func repRef(tables []string, query map[string]interface{}, proxy PhProxy.PhProxy) ([]byte, error) {
	proposalId, projectId, pointOrigin, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	// 聚合往期代表的销售和 ( 代表名称，sum销售额，sum指标 )
	beforeResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
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
				"groupBy": "representative.keyword",
				"aggs": []interface{}{
					map[string]interface{}{
						"groupBy": "phase",
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

	// 转换为各个周期的销售额透视图
	phaseSalesPivot := genPivot(beforeResult,
		func(item map[string]interface{}) string { return item["representative.keyword"].(string) },
		"phase", "sum(sales)",
	)

	// 转换为各个周期的销售指标透视图
	phaseQuotaPivot := genPivot(beforeResult,
		func(item map[string]interface{}) string { return item["representative.keyword"].(string) },
		"phase", "sum(quota)",
	)

	// 取得最新的周期的代表信息
	curInfo, err := findMaxByKey(beforeResult, "phase")
	if err != nil {
		return []byte{}, err
	}

	// 计算YTD的销售额
	maxPhase := int(curInfo[0]["phase"].(float64))
	minPhase, err := findSameYear(maxPhase, pointOrigin)
	if err != nil {
		return []byte{}, err
	}
	ytdResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Resource"},
				[]interface{}{"gte", "phase", minPhase},
				[]interface{}{"lte", "phase", maxPhase},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "representative.keyword",
				"aggs": []interface{}{
					map[string]interface{}{
						"agg":   "sum",
						"field": "sales",
					},
				},
			},
		},
	})
	if err != nil {
		return []byte{}, err
	}

	// 计算最新的周期指标和销售的总和
	curTotalQuota := 0.0
	curTotalSales := 0.0
	for _, info := range curInfo {
		curTotalQuota += info["sum(quota)"].(float64)
		curTotalSales += info["sum(sales)"].(float64)
	}

	// 根据当前周期医院表的产品字段聚合出患者人数
	curHospitalResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Hospital"},
				[]interface{}{"eq", "phase", maxPhase},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "representative.keyword",
				"aggs": []interface{}{
					map[string]interface{}{
						"agg":   "sum",
						"field": "currentPatientNum",
					},
				},
			},
		},
	})

	// ( 代表名称，患者数量，指标贡献率，指标增长率，
	// 指标达成率，销售额同比增长，销售额环比增长，销售额贡献率，YTD销售额 ) + pivot sales by phase
	var curResult = make([]map[string]interface{}, 0)
	for _, info := range curInfo {
		var tmp = make(map[string]interface{}, 0)

		tmp["representative"] = info["representative.keyword"]

		_, currentPatientNumInfo := findSliceByKeys(curHospitalResult, map[string]interface{}{
			"representative.keyword": info["representative.keyword"],
		})
		tmp["current_patient_num"] = currentPatientNumInfo["sum(currentPatientNum)"]

		tmp["quota_contri"] = calcContri(info["sum(quota)"], curTotalQuota)
		_, lastPhaseInfo := findSliceByKeys(beforeResult, map[string]interface{}{
			"representative.keyword": info["representative.keyword"],
			"phase":                  info["phase"].(float64) - 1,
		})
		tmp["quota_growth"] = calcGrowth(info["sum(quota)"], lastPhaseInfo["sum(sales)"])
		tmp["quota_rate"] = calcAchieving(info["sum(sales)"], info["sum(quota)"])

		_, lastYearInfo := findSliceByKeys(beforeResult, map[string]interface{}{
			"representative.keyword": info["representative.keyword"],
			"phase":                  info["phase"].(float64) - 4,
		})
		if lastYearInfo == nil {
			tmp["year_on_year_sales"] = 0.0
		} else {
			tmp["year_on_year_sales"] = calcGrowth(info["sum(sales)"], lastYearInfo["sum(sales)"])
		}
		tmp["sales_growth"] = calcGrowth(info["sum(sales)"], lastPhaseInfo["sum(sales)"])
		tmp["sales_contri"] = calcContri(info["sum(sales)"], curTotalSales)

		_, ytdInfo := findSliceByKeys(ytdResult, map[string]interface{}{
			"representative.keyword": info["representative.keyword"],
		})
		if ytdInfo == nil {
			tmp["ytd_sales"] = 0.0
		} else {
			tmp["ytd_sales"] = ytdInfo["sum(sales)"]
		}

		pivotSales := phaseSalesPivot[info["representative.keyword"].(string)]
		for k, v := range pivotSales {
			tmp["sales_"+fmt.Sprintf("%d", int(k.(float64)))] = v
		}
		pivotQuota := phaseQuotaPivot[info["representative.keyword"].(string)]
		for k, v := range pivotQuota {
			tmp["quota_"+fmt.Sprintf("%d", int(k.(float64)))] = v
		}

		curResult = append(curResult, tmp)
	}

	return json.Marshal(curResult)
}

func repProd(tables []string, query map[string]interface{}, proxy PhProxy.PhProxy) ([]byte, error) {
	proposalId, projectId, pointOrigin, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	// 聚合往期代表下产品的销售和 ( 代表名称，sum销售额，sum指标 )
	beforeResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
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
				"groupBy": "representative.keyword",
				"aggs": []interface{}{
					map[string]interface{}{
						"groupBy": "product.keyword",
						"aggs": []interface{}{
							map[string]interface{}{
								"groupBy": "phase",
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
			},
		},
	})
	if err != nil {
		return []byte{}, err
	}

	// 转换为各个周期的销售额透视图
	phaseSalesPivot := genPivot(beforeResult,
		func(item map[string]interface{}) string {
			return item["representative.keyword"].(string) + "+" + item["product.keyword"].(string)
		},
		"phase", "sum(sales)",
	)

	// 转换为各个周期的销售指标透视图
	phaseQuotaPivot := genPivot(beforeResult,
		func(item map[string]interface{}) string {
			return item["representative.keyword"].(string) + "+" + item["product.keyword"].(string)
		},
		"phase", "sum(quota)",
	)

	// 取得最新的周期的代表信息
	curInfo, err := findMaxByKey(beforeResult, "phase")
	if err != nil {
		return []byte{}, err
	}

	// 计算YTD的销售额
	maxPhase := int(curInfo[0]["phase"].(float64))
	minPhase, err := findSameYear(maxPhase, pointOrigin)
	if err != nil {
		return []byte{}, err
	}
	ytdResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Resource"},
				[]interface{}{"gte", "phase", minPhase},
				[]interface{}{"lte", "phase", maxPhase},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "representative.keyword",
				"aggs": []interface{}{
					map[string]interface{}{
						"groupBy": "product.keyword",
						"aggs": []interface{}{
							map[string]interface{}{
								"agg":   "sum",
								"field": "sales",
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

	// 计算最新的周期指标和销售的总和
	curTotalQuota := 0.0
	curTotalSales := 0.0
	for _, info := range curInfo {
		curTotalQuota += info["sum(quota)"].(float64)
		curTotalSales += info["sum(sales)"].(float64)
	}

	// 根据当前周期医院表的产品字段聚合出患者人数
	curHospitalResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Hospital"},
				[]interface{}{"eq", "phase", maxPhase},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "representative.keyword",
				"aggs": []interface{}{
					map[string]interface{}{
						"groupBy": "product.keyword",
						"aggs": []interface{}{
							map[string]interface{}{
								"agg":   "sum",
								"field": "currentPatientNum",
							},
						},
					},
				},
			},
		},
	})

	// ( 代表名称，患者数量，指标贡献率，指标增长率，
	// 指标达成率，销售额同比增长，销售额环比增长，销售额贡献率，YTD销售额 ) + pivot sales by phase
	var curResult = make([]map[string]interface{}, 0)
	for _, info := range curInfo {
		var tmp = make(map[string]interface{}, 0)

		tmp["representative"] = info["representative.keyword"]
		tmp["product"] = info["product.keyword"]

		_, currentPatientNumInfo := findSliceByKeys(curHospitalResult, map[string]interface{}{
			"representative.keyword": info["representative.keyword"],
			"product.keyword":        info["product.keyword"],
		})
		tmp["current_patient_num"] = currentPatientNumInfo["sum(currentPatientNum)"]

		tmp["quota_contri"] = calcContri(info["sum(quota)"], curTotalQuota)
		_, lastPhaseInfo := findSliceByKeys(beforeResult, map[string]interface{}{
			"representative.keyword": info["representative.keyword"],
			"product.keyword":        info["product.keyword"],
			"phase":                  info["phase"].(float64) - 1,
		})
		tmp["quota_growth"] = calcGrowth(info["sum(quota)"], lastPhaseInfo["sum(sales)"])
		tmp["quota_rate"] = calcAchieving(info["sum(sales)"], info["sum(quota)"])

		_, lastYearInfo := findSliceByKeys(beforeResult, map[string]interface{}{
			"representative.keyword": info["representative.keyword"],
			"product.keyword":        info["product.keyword"],
			"phase":                  info["phase"].(float64) - 4,
		})
		if lastYearInfo == nil {
			tmp["year_on_year_sales"] = 0.0
		} else {
			tmp["year_on_year_sales"] = calcGrowth(info["sum(sales)"], lastYearInfo["sum(sales)"])
		}
		tmp["sales_growth"] = calcGrowth(info["sum(sales)"], lastPhaseInfo["sum(sales)"])
		tmp["sales_contri"] = calcContri(info["sum(sales)"], curTotalSales)

		_, ytdInfo := findSliceByKeys(ytdResult, map[string]interface{}{
			"representative.keyword": info["representative.keyword"],
			"product.keyword":        info["product.keyword"],
		})
		if ytdInfo == nil {
			tmp["ytd_sales"] = 0.0
		} else {
			tmp["ytd_sales"] = ytdInfo["sum(sales)"]
		}

		pivotSales := phaseSalesPivot[info["representative.keyword"].(string)+"+"+info["product.keyword"].(string)]
		for k, v := range pivotSales {
			tmp["sales_"+fmt.Sprintf("%d", int(k.(float64)))] = v
		}
		pivotQuota := phaseQuotaPivot[info["representative.keyword"].(string)+"+"+info["product.keyword"].(string)]
		for k, v := range pivotQuota {
			tmp["quota_"+fmt.Sprintf("%d", int(k.(float64)))] = v
		}

		curResult = append(curResult, tmp)
	}

	return json.Marshal(curResult)
}

func hospitalRef(tables []string, query map[string]interface{}, proxy PhProxy.PhProxy) ([]byte, error) {
	proposalId, projectId, pointOrigin, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	// 获得往期全部信息 ( 医院名称，产品，代表，其他 )
	beforeResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
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
							map[string]interface{}{
								"agg":   "sum",
								"field": "currentPatientNum",
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

	// 转换为各个周期的销售额透视图
	phaseSalesPivot := genPivot(beforeResult,
		func(item map[string]interface{}) string { return item["hospital.keyword"].(string) },
		"phase", "sum(sales)",
	)

	// 转换为各个周期的销售指标透视图
	phaseQuotaPivot := genPivot(beforeResult,
		func(item map[string]interface{}) string { return item["hospital.keyword"].(string) },
		"phase", "sum(quota)",
	)

	// 取得最新的周期的医院信息
	maxInfo, err := findMaxByKey(beforeResult, "phase")
	if err != nil {
		return []byte{}, err
	}
	maxPhase := int(maxInfo[0]["phase"].(float64))
	curInfo, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Hospital"},
				[]interface{}{"eq", "phase", maxPhase},
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
								"groupBy": "hospital_level.keyword",
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
											map[string]interface{}{
												"agg":   "sum",
												"field": "currentPatientNum",
											},
										},
									},
								},
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

	// 计算YTD的销售额
	minPhase, err := findSameYear(maxPhase, pointOrigin)
	if err != nil {
		return []byte{}, err
	}
	ytdResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Hospital"},
				[]interface{}{"gte", "phase", minPhase},
				[]interface{}{"lte", "phase", maxPhase},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "hospital.keyword",
				"aggs": []interface{}{
					map[string]interface{}{
						"agg":   "sum",
						"field": "sales",
					},
				},
			},
		},
	})
	if err != nil {
		return []byte{}, err
	}

	// 计算最新的周期指标和销售的总和
	curTotalQuota := 0.0
	curTotalSales := 0.0
	for _, info := range curInfo {
		curTotalQuota += info["sum(quota)"].(float64)
		curTotalSales += info["sum(sales)"].(float64)
	}

	// 分出院内和院外两种医院
	sepHosp := func(data []map[string]interface{}) (internal []map[string]interface{}, outer []map[string]interface{}) {
		internal = make([]map[string]interface{}, 0)
		outer = make([]map[string]interface{}, 0)
		for _, item := range curInfo {
			if item["hospital_level.keyword"].(string) == "院外" {
				outer = append(outer, item)
			} else {
				internal = append(internal, item)
			}
		}
		return
	}
	internal, outer := sepHosp(curInfo)

	// ( 医院名称，产品，代表，患者数量，准入情况，指标贡献率，指标增长率，
	// 指标达成率，销售额同比增长，销售额环比增长，销售额贡献率，YTD销售额 ) + pivot sales by phase
	var curResult = make([]map[string]interface{}, 0)
	for _, info := range internal {
		var tmp = make(map[string]interface{}, 0)
		_, outerInfo := findSliceByKeys(outer, map[string]interface{}{
			"hospital.keyword": info["hospital.keyword"],
		})

		tmp["hospital"] = info["hospital.keyword"]
		tmp["representative"] = info["representative.keyword"]

		if outerInfo == nil { // 没有对应的外部医院
			_, lastPhaseInfo := findSliceByKeys(beforeResult, map[string]interface{}{
				"hospital.keyword": info["hospital.keyword"],
				"phase":            info["phase"].(float64) - 1,
			})
			_, lastYearInfo := findSliceByKeys(beforeResult, map[string]interface{}{
				"hospital.keyword": info["hospital.keyword"],
				"phase":            info["phase"].(float64) - 4,
			})
			_, ytdInfo := findSliceByKeys(ytdResult, map[string]interface{}{
				"hospital.keyword": info["hospital.keyword"],
			})

			tmp["current_patient_num"] = info["sum(currentPatientNum)"]

			tmp["quota_contri"] = calcContri(info["sum(quota)"], curTotalQuota)
			tmp["quota_growth"] = calcGrowth(info["sum(quota)"], lastPhaseInfo["sum(sales)"])
			tmp["quota_rate"] = calcAchieving(info["sum(sales)"], info["sum(quota)"])

			if lastYearInfo == nil {
				tmp["year_on_year_sales"] = 0.0
			} else {
				tmp["year_on_year_sales"] = calcGrowth(info["sum(sales)"], lastYearInfo["sum(sales)"])
			}
			tmp["sales_growth"] = calcGrowth(info["sum(sales)"], lastPhaseInfo["sum(sales)"])
			tmp["sales_contri"] = calcContri(info["sum(sales)"], curTotalSales)
			if ytdInfo == nil {
				tmp["ytd_sales"] = 0.0
			} else {
				tmp["ytd_sales"] = ytdInfo["sum(sales)"]
			}

			tmp["inter_sales"] = info["sum(sales)"]
			tmp["outer_sales"] = 0.0
		} else {
			_, lastPhaseInfo := findSliceByKeys(beforeResult, map[string]interface{}{
				"hospital.keyword": info["hospital.keyword"],
				"phase":            info["phase"].(float64) - 1,
			})
			_, lastYearInfo := findSliceByKeys(beforeResult, map[string]interface{}{
				"hospital.keyword": info["hospital.keyword"],
				"phase":            info["phase"].(float64) - 4,
			})
			_, ytdInfo := findSliceByKeys(ytdResult, map[string]interface{}{
				"hospital.keyword": info["hospital.keyword"],
			})

			tmp["current_patient_num"] = info["sum(currentPatientNum)"].(float64) + outerInfo["sum(currentPatientNum)"].(float64)

			tmp["quota_contri"] = calcContri(info["sum(quota)"].(float64)+outerInfo["sum(quota)"].(float64), curTotalQuota)
			tmp["quota_growth"] = calcGrowth(info["sum(quota)"].(float64)+outerInfo["sum(quota)"].(float64), lastPhaseInfo["sum(sales)"])
			tmp["quota_rate"] = calcAchieving(info["sum(sales)"].(float64)+outerInfo["sum(sales)"].(float64), info["sum(quota)"].(float64)+outerInfo["sum(quota)"].(float64))

			if lastYearInfo == nil {
				tmp["year_on_year_sales"] = 0.0
			} else {
				tmp["year_on_year_sales"] = calcGrowth(info["sum(sales)"].(float64)+outerInfo["sum(sales)"].(float64), lastYearInfo["sum(sales)"])
			}
			tmp["sales_growth"] = calcGrowth(info["sum(sales)"].(float64)+outerInfo["sum(sales)"].(float64), lastPhaseInfo["sum(sales)"])
			tmp["sales_contri"] = calcContri(info["sum(sales)"].(float64)+outerInfo["sum(sales)"].(float64), curTotalSales)
			if ytdInfo == nil {
				tmp["ytd_sales"] = 0.0
			} else {
				tmp["ytd_sales"] = ytdInfo["sum(sales)"]
			}

			tmp["inter_sales"] = info["sum(sales)"]
			tmp["outer_sales"] = outerInfo["sum(sales)"]
		}

		pivotSales := phaseSalesPivot[info["hospital.keyword"].(string)]
		for k, v := range pivotSales {
			tmp["sales_"+fmt.Sprintf("%d", int(k.(float64)))] = v
		}
		pivotQuota := phaseQuotaPivot[info["hospital.keyword"].(string)]
		for k, v := range pivotQuota {
			tmp["quota_"+fmt.Sprintf("%d", int(k.(float64)))] = v
		}

		curResult = append(curResult, tmp)
	}

	return json.Marshal(curResult)
}

func hospitalProd(tables []string, query map[string]interface{}, proxy PhProxy.PhProxy) ([]byte, error) {
	proposalId, projectId, pointOrigin, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	// 获得往期全部信息 ( 医院名称，产品，代表，其他 )
	beforeResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
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
				"groupBy": "hospital.keyword",
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
			},
		},
	})
	if err != nil {
		return []byte{}, err
	}

	// 转换为各个周期的销售额透视图
	phaseSalesPivot := genPivot(beforeResult,
		func(item map[string]interface{}) string {
			return item["hospital.keyword"].(string) + "+" + item["product.keyword"].(string)
		},
		"phase", "sum(sales)",
	)

	// 转换为各个周期的销售指标透视图
	phaseQuotaPivot := genPivot(beforeResult,
		func(item map[string]interface{}) string {
			return item["hospital.keyword"].(string) + "+" + item["product.keyword"].(string)
		},
		"phase", "sum(quota)",
	)

	// 取得最新的周期的医院信息
	maxInfo, err := findMaxByKey(beforeResult, "phase")
	if err != nil {
		return []byte{}, err
	}
	maxPhase := int(maxInfo[0]["phase"].(float64))
	curInfo, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Hospital"},
				[]interface{}{"eq", "phase", maxPhase},
			},
		},
	})

	// 计算YTD的销售额
	minPhase, err := findSameYear(maxPhase, pointOrigin)
	if err != nil {
		return []byte{}, err
	}
	ytdResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Hospital"},
				[]interface{}{"gte", "phase", minPhase},
				[]interface{}{"lte", "phase", maxPhase},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "hospital.keyword",
				"aggs": []interface{}{
					map[string]interface{}{
						"groupBy": "product.keyword",
						"aggs": []interface{}{
							map[string]interface{}{
								"agg":   "sum",
								"field": "sales",
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

	// 计算最新的周期指标和销售的总和
	curTotalQuota := 0.0
	curTotalSales := 0.0
	for _, info := range curInfo {
		curTotalQuota += info["quota"].(float64)
		curTotalSales += info["sales"].(float64)
	}

	// 分出院内和院外两种医院
	sepHosp := func(data []map[string]interface{}) (internal []map[string]interface{}, outer []map[string]interface{}) {
		internal = make([]map[string]interface{}, 0)
		outer = make([]map[string]interface{}, 0)
		for _, item := range curInfo {
			if item["hospital_level"].(string) == "院外" {
				outer = append(outer, item)
			} else {
				internal = append(internal, item)
			}
		}
		return
	}
	internal, outer := sepHosp(curInfo)

	// ( 医院名称，产品，代表，患者数量，准入情况，指标贡献率，指标增长率，
	// 指标达成率，销售额同比增长，销售额环比增长，销售额贡献率，YTD销售额 ) + pivot sales by phase
	var curResult = make([]map[string]interface{}, 0)
	for _, info := range internal {
		var tmp = make(map[string]interface{}, 0)
		_, outerInfo := findSliceByKeys(outer, map[string]interface{}{
			"hospital": info["hospital"],
			"product":  info["product"],
		})
		_, lastPhaseInfo := findSliceByKeys(beforeResult, map[string]interface{}{
			"hospital.keyword": info["hospital"],
			"product.keyword":  info["product"],
			"phase":            info["phase"].(float64) - 1,
		})
		_, lastYearInfo := findSliceByKeys(beforeResult, map[string]interface{}{
			"hospital.keyword": info["hospital"],
			"product.keyword":  info["product"],
			"phase":            info["phase"].(float64) - 4,
		})
		_, ytdInfo := findSliceByKeys(ytdResult, map[string]interface{}{
			"hospital.keyword": info["hospital"],
			"product.keyword":  info["product"],
		})

		tmp["hospital"] = info["hospital"]
		tmp["product"] = info["product"]
		tmp["representative"] = info["representative"]
		tmp["status"] = info["status"]

		if outerInfo == nil { // 没有对应的外部医院
			tmp["current_patient_num"] = info["currentPatientNum"]

			tmp["quota_contri"] = calcContri(info["quota"], curTotalQuota)
			tmp["quota_growth"] = calcGrowth(info["quota"], lastPhaseInfo["sum(sales)"])
			tmp["quota_rate"] = calcAchieving(info["sales"], info["quota"])

			if lastYearInfo == nil {
				tmp["year_on_year_sales"] = 0.0
			} else {
				tmp["year_on_year_sales"] = calcGrowth(info["sales"], lastYearInfo["sum(sales)"])
			}
			tmp["sales_growth"] = calcGrowth(info["sales"], lastPhaseInfo["sum(sales)"])
			tmp["sales_contri"] = calcContri(info["sales"], curTotalSales)
			if ytdInfo == nil {
				tmp["ytd_sales"] = 0.0
			} else {
				tmp["ytd_sales"] = ytdInfo["sum(sales)"]
			}

			tmp["inter_sales"] = info["sales"]
			tmp["outer_sales"] = 0.0
		} else {
			tmp["current_patient_num"] = info["currentPatientNum"].(float64) + outerInfo["currentPatientNum"].(float64)

			tmp["quota_contri"] = calcContri(info["quota"].(float64)+outerInfo["quota"].(float64), curTotalQuota)
			tmp["quota_growth"] = calcGrowth(info["quota"].(float64)+outerInfo["quota"].(float64), lastPhaseInfo["sum(sales)"])
			tmp["quota_rate"] = calcAchieving(info["sales"].(float64)+outerInfo["sales"].(float64), info["quota"].(float64)+outerInfo["quota"].(float64))

			if lastYearInfo == nil {
				tmp["year_on_year_sales"] = 0.0
			} else {
				tmp["year_on_year_sales"] = calcGrowth(info["sales"].(float64)+outerInfo["sales"].(float64), lastYearInfo["sum(sales)"])
			}
			tmp["sales_growth"] = calcGrowth(info["sales"].(float64)+outerInfo["sales"].(float64), lastPhaseInfo["sum(sales)"])
			tmp["sales_contri"] = calcContri(info["sales"].(float64)+outerInfo["sales"].(float64), curTotalSales)
			if ytdInfo == nil {
				tmp["ytd_sales"] = 0.0
			} else {
				tmp["ytd_sales"] = ytdInfo["sum(sales)"]
			}

			tmp["inter_sales"] = info["sales"]
			tmp["outer_sales"] = outerInfo["sales"]
		}

		pivotSales := phaseSalesPivot[info["hospital"].(string)+"+"+info["product"].(string)]
		for k, v := range pivotSales {
			tmp["sales_"+fmt.Sprintf("%d", int(k.(float64)))] = v
		}
		pivotQuota := phaseQuotaPivot[info["hospital"].(string)+"+"+info["product"].(string)]
		for k, v := range pivotQuota {
			tmp["quota_"+fmt.Sprintf("%d", int(k.(float64)))] = v
		}

		curResult = append(curResult, tmp)
	}

	return json.Marshal(curResult)
}

func regionRef(tables []string, query map[string]interface{}, proxy PhProxy.PhProxy) ([]byte, error) {
	proposalId, projectId, pointOrigin, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	// 聚合往期区域的销售和 ( 区域名称，sum销售额，sum指标 )
	beforeResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Region"},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "region.keyword",
				"aggs": []interface{}{
					map[string]interface{}{
						"groupBy": "phase",
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

	// 转换为各个周期的销售额透视图
	phaseSalesPivot := genPivot(beforeResult,
		func(item map[string]interface{}) string { return item["region.keyword"].(string) },
		"phase", "sum(sales)",
	)

	// 转换为各个周期的销售指标透视图
	phaseQuotaPivot := genPivot(beforeResult,
		func(item map[string]interface{}) string { return item["region.keyword"].(string) },
		"phase", "sum(quota)",
	)

	// 取得最新的周期的代表信息
	curInfo, err := findMaxByKey(beforeResult, "phase")
	if err != nil {
		return []byte{}, err
	}

	// 计算YTD的销售额
	maxPhase := int(curInfo[0]["phase"].(float64))
	minPhase, err := findSameYear(maxPhase, pointOrigin)
	if err != nil {
		return []byte{}, err
	}
	ytdResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Region"},
				[]interface{}{"gte", "phase", minPhase},
				[]interface{}{"lte", "phase", maxPhase},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "region.keyword",
				"aggs": []interface{}{
					map[string]interface{}{
						"agg":   "sum",
						"field": "sales",
					},
				},
			},
		},
	})
	if err != nil {
		return []byte{}, err
	}

	// 计算最新的周期指标和销售的总和
	curTotalQuota := 0.0
	curTotalSales := 0.0
	for _, info := range curInfo {
		curTotalQuota += info["sum(quota)"].(float64)
		curTotalSales += info["sum(sales)"].(float64)
	}

	// 根据当前周期医院表的代表字段聚合出患者人数
	curHospitalResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Hospital"},
				[]interface{}{"eq", "phase", maxPhase},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "region.keyword",
				"aggs": []interface{}{
					map[string]interface{}{
						"agg":   "sum",
						"field": "currentPatientNum",
					},
				},
			},
		},
	})

	// ( 代表名称，患者数量，指标贡献率，指标增长率，
	// 指标达成率，销售额同比增长，销售额环比增长，销售额贡献率，YTD销售额 ) + pivot sales by phase
	var curResult = make([]map[string]interface{}, 0)
	for _, info := range curInfo {
		var tmp = make(map[string]interface{}, 0)

		tmp["region"] = info["region.keyword"]
		_, currentPatientNumInfo := findSliceByKeys(curHospitalResult, map[string]interface{}{
			"region.keyword": info["region.keyword"],
		})
		tmp["current_patient_num"] = currentPatientNumInfo["sum(currentPatientNum)"]

		tmp["quota_contri"] = calcContri(info["sum(quota)"], curTotalQuota)
		_, lastPhaseInfo := findSliceByKeys(beforeResult, map[string]interface{}{
			"region.keyword": info["region.keyword"],
			"phase":          info["phase"].(float64) - 1,
		})
		tmp["quota_growth"] = calcGrowth(info["sum(quota)"], lastPhaseInfo["sum(sales)"])
		tmp["quota_rate"] = calcAchieving(info["sum(sales)"], info["sum(quota)"])

		_, lastYearInfo := findSliceByKeys(beforeResult, map[string]interface{}{
			"region.keyword": info["region.keyword"],
			"phase":          info["phase"].(float64) - 4,
		})
		if lastYearInfo == nil {
			tmp["year_on_year_sales"] = 0.0
		} else {
			tmp["year_on_year_sales"] = calcGrowth(info["sum(sales)"], lastYearInfo["sum(sales)"])
		}
		tmp["sales_growth"] = calcGrowth(info["sum(sales)"], lastPhaseInfo["sum(sales)"])
		tmp["sales_contri"] = calcContri(info["sum(sales)"], curTotalSales)

		_, ytdInfo := findSliceByKeys(ytdResult, map[string]interface{}{
			"region.keyword": info["region.keyword"],
		})
		if ytdInfo == nil {
			tmp["ytd_sales"] = 0.0
		} else {
			tmp["ytd_sales"] = ytdInfo["sum(sales)"]
		}

		pivotSales := phaseSalesPivot[info["region.keyword"].(string)]
		for k, v := range pivotSales {
			tmp["sales_"+fmt.Sprintf("%d", int(k.(float64)))] = v
		}
		pivotQuota := phaseQuotaPivot[info["region.keyword"].(string)]
		for k, v := range pivotQuota {
			tmp["quota_"+fmt.Sprintf("%d", int(k.(float64)))] = v
		}

		curResult = append(curResult, tmp)
	}

	return json.Marshal(curResult)
}

func regionProd(tables []string, query map[string]interface{}, proxy PhProxy.PhProxy) ([]byte, error) {
	proposalId, projectId, pointOrigin, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	// 聚合往期区域的销售和 ( 区域名称，sum销售额，sum指标 )
	beforeResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Region"},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "region.keyword",
				"aggs": []interface{}{
					map[string]interface{}{
						"groupBy": "product.keyword",
						"aggs": []interface{}{
							map[string]interface{}{
								"groupBy": "phase",
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
			},
		},
	})
	if err != nil {
		return []byte{}, err
	}

	// 转换为各个周期的销售额透视图
	phaseSalesPivot := genPivot(beforeResult,
		func(item map[string]interface{}) string {
			return item["region.keyword"].(string) + "+" + item["product.keyword"].(string)
		},
		"phase", "sum(sales)",
	)

	// 转换为各个周期的销售指标透视图
	phaseQuotaPivot := genPivot(beforeResult,
		func(item map[string]interface{}) string {
			return item["region.keyword"].(string) + "+" + item["product.keyword"].(string)
		},
		"phase", "sum(quota)",
	)

	// 取得最新的周期的代表信息
	curInfo, err := findMaxByKey(beforeResult, "phase")
	if err != nil {
		return []byte{}, err
	}

	// 计算YTD的销售额
	maxPhase := int(curInfo[0]["phase"].(float64))
	minPhase, err := findSameYear(maxPhase, pointOrigin)
	if err != nil {
		return []byte{}, err
	}
	ytdResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Region"},
				[]interface{}{"gte", "phase", minPhase},
				[]interface{}{"lte", "phase", maxPhase},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "region.keyword",
				"aggs": []interface{}{
					map[string]interface{}{
						"groupBy": "product.keyword",
						"aggs": []interface{}{
							map[string]interface{}{
								"agg":   "sum",
								"field": "sales",
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

	// 计算最新的周期指标和销售的总和
	curTotalQuota := 0.0
	curTotalSales := 0.0
	for _, info := range curInfo {
		curTotalQuota += info["sum(quota)"].(float64)
		curTotalSales += info["sum(sales)"].(float64)
	}

	// 根据当前周期医院表的代表字段聚合出患者人数
	curHospitalResult, err := proxy.Read(tables, map[string]interface{}{
		"search": map[string]interface{}{
			"size": 10000.0,
			"and": []interface{}{
				[]interface{}{"or", []interface{}{
					[]interface{}{"eq", "proposal_id.keyword", proposalId},
					[]interface{}{"eq", "project_id.keyword", projectId},
				}},
				[]interface{}{"eq", "category.keyword", "Hospital"},
				[]interface{}{"eq", "phase", maxPhase},
			},
		},
		"aggs": []interface{}{
			map[string]interface{}{
				"groupBy": "region.keyword",
				"aggs": []interface{}{
					map[string]interface{}{
						"groupBy": "product.keyword",
						"aggs": []interface{}{
							map[string]interface{}{
								"agg":   "sum",
								"field": "currentPatientNum",
							},
						},
					},
				},
			},
		},
	})

	// ( 代表名称，患者数量，指标贡献率，指标增长率，
	// 指标达成率，销售额同比增长，销售额环比增长，销售额贡献率，YTD销售额 ) + pivot sales by phase
	var curResult = make([]map[string]interface{}, 0)
	for _, info := range curInfo {
		var tmp = make(map[string]interface{}, 0)

		tmp["region"] = info["region.keyword"]
		tmp["product"] = info["product.keyword"]

		_, currentPatientNumInfo := findSliceByKeys(curHospitalResult, map[string]interface{}{
			"region.keyword":  info["region.keyword"],
			"product.keyword": info["product.keyword"],
		})
		tmp["current_patient_num"] = currentPatientNumInfo["sum(currentPatientNum)"]

		tmp["quota_contri"] = calcContri(info["sum(quota)"], curTotalQuota)
		_, lastPhaseInfo := findSliceByKeys(beforeResult, map[string]interface{}{
			"region.keyword":  info["region.keyword"],
			"product.keyword": info["product.keyword"],
			"phase":           info["phase"].(float64) - 1,
		})
		tmp["quota_growth"] = calcGrowth(info["sum(quota)"], lastPhaseInfo["sum(sales)"])
		tmp["quota_rate"] = calcAchieving(info["sum(sales)"], info["sum(quota)"])

		_, lastYearInfo := findSliceByKeys(beforeResult, map[string]interface{}{
			"region.keyword":  info["region.keyword"],
			"product.keyword": info["product.keyword"],
			"phase":           info["phase"].(float64) - 4,
		})
		if lastYearInfo == nil {
			tmp["year_on_year_sales"] = 0.0
		} else {
			tmp["year_on_year_sales"] = calcGrowth(info["sum(sales)"], lastYearInfo["sum(sales)"])
		}
		tmp["sales_growth"] = calcGrowth(info["sum(sales)"], lastPhaseInfo["sum(sales)"])
		tmp["sales_contri"] = calcContri(info["sum(sales)"], curTotalSales)

		_, ytdInfo := findSliceByKeys(ytdResult, map[string]interface{}{
			"region.keyword":  info["region.keyword"],
			"product.keyword": info["product.keyword"],
		})
		if ytdInfo == nil {
			tmp["ytd_sales"] = 0.0
		} else {
			tmp["ytd_sales"] = ytdInfo["sum(sales)"]
		}

		pivotSales := phaseSalesPivot[info["region.keyword"].(string) + "+" + info["product.keyword"].(string)]
		for k, v := range pivotSales {
			tmp["sales_"+fmt.Sprintf("%d", int(k.(float64)))] = v
		}
		pivotQuota := phaseQuotaPivot[info["region.keyword"].(string) + "+" + info["product.keyword"].(string)]
		for k, v := range pivotQuota {
			tmp["quota_"+fmt.Sprintf("%d", int(k.(float64)))] = v
		}

		curResult = append(curResult, tmp)
	}

	return json.Marshal(curResult)
}

// 解析查询参数
func parseQuery(query map[string]interface{}) (proposalId, projectId, pointOrigin string, err error) {
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

	if ok := query["point_origin"]; ok == nil {
		if ok := os.Getenv("POINT_ORIGIN"); ok == "" {
			pointOrigin = "2019Q1"
		} else {
			pointOrigin = ok
		}
	} else {
		pointOrigin = ok.(string)
	}
	return
}

// 比较 prev 是否 大于 next
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

// 根据map中的key找到最大的一组元素
func findMaxByKey(data []map[string]interface{}, key string) ([]map[string]interface{}, error) {
	if len(data) < 1 {
		return nil, nil
	}

	tmpMap := make(map[interface{}][]map[string]interface{}, 0)
	for _, item := range data {
		if s, ok := tmpMap[item[key]]; ok {
			tmpMap[item[key]] = append(s, item)
		} else {
			tmpMap[item[key]] = []map[string]interface{}{item}
		}
	}

	var head = true
	var max interface{}
	for k := range tmpMap {
		if head {
			max = k
			head = false
			continue
		}

		if b, err := compare(k, max); err != nil {
			return nil, err
		} else if b {
			max = k
		}
	}

	return tmpMap[max], nil
}

// 安全除法
func safeDivision(dividend, divisor float64) float64 {
	if divisor == 0.0 {
		if dividend == 0.0 {
			return 0.0
		} else {
			return 1.0
		}
	} else {
		return dividend / divisor
	}
}

// 根据条件找到元素
func findSliceByKeys(slice []map[string]interface{}, keys map[string]interface{}) (int, map[string]interface{}) {
	for i, item := range slice {
		exist := true
		for k, v := range keys {
			if item[k] != v {
				exist = false
				break
			}
		}
		if exist {
			return i, item
		}
	}
	return -1, nil
}

// 找到属于同一年的最小周期
func findSameYear(phase int, pointOrigin string) (int, error) {
	pYear, err := strconv.Atoi(pointOrigin[:4])
	if err != nil {
		return 0, err
	}
	pQuarte, err := strconv.Atoi(pointOrigin[5:6])
	if err != nil {
		return 0, err
	}

	dYear := phase / 4
	dQuarter := phase % 4

	var curYear, curQuarter int
	curYear = pYear + dYear
	curQuarter = pQuarte + dQuarter
	if curQuarter > 4 {
		curYear += 1
		curQuarter -= 4
	} else if curQuarter < 1 {
		curYear -= 1
		curQuarter += 4
	}

	return phase + 1 - curQuarter, nil
}

// 计算贡献率
func calcContri(value, total interface{}) float64 {
	return safeDivision(value.(float64), total.(float64))
}

// 计算增长率
func calcGrowth(cur, last interface{}) float64 {
	tmp := safeDivision(cur.(float64), last.(float64))
	if tmp == 0.0 {
		return tmp
	} else {
		return tmp - 1
	}
}

// 计算达成率
func calcAchieving(sales, quota interface{}) float64 {
	return safeDivision(sales.(float64), quota.(float64))
}

// 转换为透视图
func genPivot(data []map[string]interface{}, pkFunc func(item map[string]interface{}) string, key string, value string) map[string]map[interface{}]interface{} {
	phaseSalesPivot := make(map[string]map[interface{}]interface{}, 0)
	for _, item := range data {
		primaryKey := pkFunc(item)
		if data, ok := phaseSalesPivot[primaryKey]; ok {
			data[item[key]] = item[value]
			phaseSalesPivot[primaryKey] = data
		} else {
			tmp := make(map[interface{}]interface{}, 0)
			tmp[item[key]] = item[value]
			phaseSalesPivot[primaryKey] = tmp
		}
	}
	return phaseSalesPivot
}
