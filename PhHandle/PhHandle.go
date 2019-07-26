package PhHandle

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/PharbersDeveloper/DL/PhProxy"
)

func PhHandle(proxy PhProxy.PhProxy) (handler func(http.ResponseWriter, *http.Request)) {
	return func(w http.ResponseWriter, r *http.Request) {
		reqMethod := r.Method
		var response []byte

		switch reqMethod {
		case "GET":
			data, err := proxy.Read(map[string]interface{}{
				"request": r,
			})
			if err != nil {
				panic(err)
			}
			if v, ok := data["error"]; ok {
				panic(v)
			}
			response, err = toBytes(data)

			title, err := extractTitle(r)
			if len(title) > 0 {
				data["title"] = title
				format, err := proxy.Format(data)
				if err != nil {
					panic(err)
				}
				response, err = toBytes(format)
				if err != nil {
					panic(err)
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

func extractTitle(r *http.Request) (title []string, err error) {
	queryArray := strings.Split(r.URL.RawQuery, "&")

	var existFormat = false
	for _, v := range queryArray {
		if "format" == v {
			existFormat = true
		}
	}

	if existFormat {
		for _, v := range queryArray {
			tmp := strings.Split(v, "=")
			if "_source" == tmp[0] {
				title = strings.Split(tmp[1], ",")
				break
			}
		}
	}

	return
}

func toBytes(data interface{}) (bytes []byte, err error) {
	return json.Marshal(data)
}
