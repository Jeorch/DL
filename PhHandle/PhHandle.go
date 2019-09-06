package PhHandle

import (
	"encoding/json"
	"github.com/PharbersDeveloper/DL/PhModel"
	"github.com/PharbersDeveloper/DL/PhProxy"
	"github.com/PharbersDeveloper/bp-go-lib/log"
	"net/http"
	"strings"
)

func PhHandle(proxy PhProxy.PhProxy) (handler func(http.ResponseWriter, *http.Request)) {
	bpLog := log.NewLogicLoggerBuilder().Build()

	return func(w http.ResponseWriter, r *http.Request) {
		var response []byte
		var model PhModel.PhModel

		if err := extractModel(r, &model); err != nil || model.IsEmpty() {
			bpLog.Error("Error of the model : " + r.URL.RawQuery)
			response = []byte("Error of the model : " + r.URL.RawQuery)
		} else {
			bpLog.Infof("%s : %#v", r.Method, model)

			switch r.Method {
			case "PUT":
				response = []byte("Not Supported")
			case "DELETE":
				response = []byte("Not Supported")
			case "POST":
				response = []byte("Not Supported")
			case "GET":
				bpLog.Info("开始查询 table='%s', cond='%#v'", model.Model, model.Query)

				tables := strings.Split(model.Model, ",")
				if readResult, err := proxy.Read(tables, model.Query); err != nil {
					bpLog.Error("Query Error: " + err.Error())
					response = []byte("Query Error: " + err.Error())
				} else {
					if formatResult, err := model.FormatResult(readResult); err != nil {
						bpLog.Error("Format Error: " + err.Error())
						response = []byte("Format Error: " + err.Error())
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
