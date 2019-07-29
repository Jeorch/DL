package PhHandle

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/PharbersDeveloper/DL/PhProxy"
)

func PhHandle(proxy PhProxy.PhProxy) (handler func(http.ResponseWriter, *http.Request)) {
	return func(w http.ResponseWriter, r *http.Request) {
		var response []byte
		reqMethod := r.Method
		model := strings.Split(r.URL.Path, "/")[1]

		switch reqMethod {
		case "GET":
			queryObj, err := extractQueryParam(r)

			// Read
			data, err := proxy.Read(map[string]interface{}{
				"model": model,
				"body":  queryObj,
			})
			if err != nil {
				panic(err)
			}
			if v, ok := data["error"]; ok {
				panic(v)
			}
			response, err = json.Marshal(data)

			// 如果需要Format
			if source, ok := queryObj["_source"]; ok && existFormatParam(r) {
				var title []string
				for _, item := range source.([]interface{}) {
					title = append(title, item.(string))
				}
				if len(title) > 0 {
					formated, err := proxy.Format(map[string]interface{}{
						"data":  data,
						"title": title,
					})
					if err != nil {
						panic(err)
					}
					response, err = json.Marshal(formated)
				}
			}
		default:
			response = []byte("Bad Request")
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

func extractQueryParam(r *http.Request) (queryObj map[string]interface{}, err error) {
	queryObj = make(map[string]interface{})

	urlQueryObj, err := extractUrlQuery(r)
	if err != nil {
		return
	}
	for k, v := range urlQueryObj {
		queryObj[k] = v
	}

	bodyQueryObj, err := extractBodyQuery(r)
	if err != nil || bodyQueryObj == nil {
		return
	}
	for k, v := range bodyQueryObj {
		queryObj[k] = v
	}

	return
}

func extractUrlQuery(r *http.Request) (queryObj map[string]interface{}, err error) {
	queryString, err := url.QueryUnescape(r.URL.RawQuery)
	if err != nil {
		return
	}
	if "" == queryString {
		return
	}

	if err = json.Unmarshal([]byte(queryString), &queryObj); err != nil {
		return
	}
	return
}

func extractBodyQuery(r *http.Request) (queryObj map[string]interface{}, err error) {
	decoder := json.NewDecoder(r.Body)
	err = decoder.Decode(&queryObj)
	return
}
