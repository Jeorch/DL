package PhHandle

import (
	"strings"
	"net/http"
	"encoding/json"

	"github.com/PharbersDeveloper/DL/PhProxy"
)

func PhHandle(proxy PhProxy.PhProxy) (handler func(http.ResponseWriter, *http.Request)) {
	return func(w http.ResponseWriter, r *http.Request) {
		reqMethod := r.Method
		reqModel := strings.Split(r.URL.Path, "/")[1]

		var response []byte

		switch reqMethod {

		case "GET":
			data, err := proxy.Read(map[string]interface{}{
				"model":   reqModel,
				"request": r,
			})
			if err != nil {
				panic(err)
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
	pathArray := strings.Split(r.URL.Path, "/")
	if len(pathArray) > 2 && pathArray[2] == "format" {
		for _, v := range strings.Split(r.URL.RawQuery, "&") {
			tmp := strings.Split(v, "=")
			if "_source" == tmp[0] {
				title = strings.Split(tmp[1], ",")
			}
		}
	}
	return
}

func toBytes(data interface{}) (bytes []byte, err error) {
	return json.Marshal(data)
}
