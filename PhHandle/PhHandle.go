package PhHandle

import (
	"encoding/json"
	"github.com/PharbersDeveloper/DL/PhModel"
	"github.com/PharbersDeveloper/DL/PhProxy"
	"net/http"
	"strings"
)

func PhHandle(proxy PhProxy.PhProxy) (handler func(http.ResponseWriter, *http.Request)) {
	return func(w http.ResponseWriter, r *http.Request) {
		var response []byte
		var model PhModel.PhModel

		if err := extractModel(r, &model); err != nil || model.IsEmpty() {
			response = []byte("Error of the model")
		} else {
			switch r.Method {
			case "PUT":
				response = []byte("Not Supported")
			case "DELETE":
				response = []byte("Not Supported")
			case "POST":
				response = []byte("Not Supported")
			case "GET":
				if readResult, err := proxy.Read(model); err != nil {
					response = []byte("Query Error")
				} else {
					if formatResult, err := model.FormatResult(readResult); err != nil {
						response = []byte("Format Error")
					} else {
						response, err = json.Marshal(formatResult)
					}
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

func existFormatParam(r *http.Request) bool {
	pathArray := strings.Split(r.URL.Path, "/")
	var existFormat = false
	for _, v := range pathArray {
		if "format" == v {
			existFormat = true
		}
	}
	return existFormat
}

//model, err := extractModel(r)
//if err != nil {
//	response = []byte(err.Error())
//}
//fmt.Println(model)

//switch reqMethod {
//case "GET":
//var queryModel PhModel.PhModel
//decoder := json.NewDecoder(r.Body)
//_ = decoder.Decode(&queryModel)
//
//queryObj, err := extractModel(r)
//
//// Read
//data, err := proxy.Read(map[string]interface{}{
//	"model": model,
//	"body":  queryObj,
//})
//if err != nil {
//	panic(err)
//}
//if v, ok := data["error"]; ok {
//	panic(v)
//}
//response, err = json.Marshal(data)
//
//// 如果需要Format
//if source, ok := queryObj["_source"]; ok && existFormatParam(r) {
//	var title []string
//	for _, item := range source.([]interface{}) {
//		title = append(title, item.(string))
//	}
//	if len(title) > 0 {
//		formated, err := proxy.Format(map[string]interface{}{
//			"data":  data,
//			"title": title,
//		})
//		if err != nil {
//			panic(err)
//		}
//		response, err = json.Marshal(formated)
//	}
//}
//
//// 如果需要Format
//if source, ok := queryObj["_source"]; ok && existPivotParam(r) {
//	var title []string
//	for _, item := range source.([]interface{}) {
//		title = append(title, item.(string))
//	}
//	if len(title) == 3 {
//		formated, err := proxy.Format(map[string]interface{}{
//			"data":  data,
//			"title": title,
//		})
//		if err != nil {
//			panic(err)
//		}
//
//		formatSlice := formated.([][]interface{})[1:len(formated.([][]interface{}))]
//		result := make([][]interface{}, 0)
//
//		xTmpAxis := []string{title[len(title)-1]}
//		xAxis := make([]interface{}, 0)
//		for _, row := range formatSlice {
//			xTmpAxis = append(xTmpAxis, row[1].(string))
//		}
//		for i := 0; i < len(xTmpAxis); i++ {
//			repeat := false
//			for j := i + 1; j < len(xTmpAxis); j++ {
//				if xTmpAxis[i] == xTmpAxis[j] {
//					repeat = true
//					break
//				}
//			}
//			if !repeat {
//				xAxis = append(xAxis, xTmpAxis[i])
//			}
//		}
//		result = append(result, xAxis)
//
//		yTmpAxis := make([]interface{}, 0)
//		var xSize = len(xAxis)
//		var currentPoint = 0
//		for _, row := range formatSlice	{
//			if currentPoint == 0 {
//				yTmpAxis = append(yTmpAxis, row[0])
//				currentPoint += 1
//			}
//
//			yTmpAxis = append(yTmpAxis, row[2])
//			currentPoint += 1
//
//			if currentPoint == xSize {
//				result = append(result, yTmpAxis)
//				yTmpAxis = make([]interface{}, 0)
//				currentPoint = 0
//			}
//		}
//
//		response, err = json.Marshal(result)
//	}
//}
//default:
//	response = []byte("Bad Request")
//}
func existPivotParam(r *http.Request) bool {
	pathArray := strings.Split(r.URL.Path, "/")
	var existFormat = false
	for _, v := range pathArray {
		if "pivot" == v {
			existFormat = true
		}
	}
	return existFormat
}
