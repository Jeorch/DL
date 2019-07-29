package PhProxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

type ESProxy struct {
	Protocol string
	Host     string
	Port     string
	User     string
	Pwd      string
}

func (proxy ESProxy) NewProxy(args map[string]string) *ESProxy {
	protocol := args["protocol"]
	if "" == protocol {
		protocol = "http"
	}

	return &ESProxy{
		Protocol: protocol,
		Host:     args["host"],
		Port:     args["port"],
		User:     args["user"],
		Pwd:      args["pwd"],
	}
}

func (proxy ESProxy) Create(args map[string]interface{}) (data map[string]interface{}, err error) {
	return
}

func (proxy ESProxy) Update(args map[string]interface{}) (data map[string]interface{}, err error) {
	return
}

func (proxy ESProxy) Read(args map[string]interface{}) (data map[string]interface{}, err error) {
	reqMethod := "GET"
	model := args["model"].(string)
	body := args["body"]

	reqUrl := fmt.Sprintf("%s://%s:%s/%s/_doc/_search",
		proxy.Protocol,
		proxy.Host,
		proxy.Port,
		model,
	)

	return callHttp(reqMethod, reqUrl, body)
}

func (proxy ESProxy) Delete(args map[string]interface{}) (data map[string]interface{}, err error) {
	return
}

func (proxy ESProxy) Format(args map[string]interface{}) (resp interface{}, err error) {
	root := make([][]interface{}, 0)

	title := args["title"].([]string)
	data := args["data"].(map[string]interface{})
	if len(title) == 0 {
		return root, nil
	} else {
		tmp := make([]interface{}, len(title))
		for i, v := range title {
			tmp[i] = v
		}
		root = append(root, tmp)
	}

	if v, ok := data["hits"]; ok {
		if hits, ok := v.(map[string]interface{})["hits"]; ok {
			items := hits.([]interface{})
			for _, item := range items {
				arr := make([]interface{}, 0)
				if source, ok := item.(map[string]interface{})["_source"]; ok {
					obj := source.(map[string]interface{})
					for _, k := range title {
						arr = append(arr, obj[k])
					}
				}
				root = append(root, arr)
			}
		}
	}

	return root, nil
}

func callHttp(method, url string, body interface{}) (data map[string]interface{}, err error) {
	var bodyReader io.Reader
	if body != nil{
		bodyBytes, _ := json.Marshal(body)
		bodyReader = bytes.NewReader(bodyBytes)
	}

	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return
	}

	req.Header.Add("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}

	respBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}
	json.Unmarshal(respBody, &data)

	return
}

func parse2json(query string) (string, error) {
	var tmpMap = make(map[string]interface{}, 0)
	queryArray := strings.Split(query, "&")

	for _, v := range queryArray {
		var param = strings.Split(v, "=")
		if len(param) < 2 || param[1] == "" {
			break
		}

		switch param[0] {
		case "_source":
			var tmp = make([]string, 0)
			for _, v := range strings.Split(param[1], ",") {
				tmp = append(tmp, v)
			}
			tmpMap["_source"] = tmp
		case "sort":
			var tmp = make([]map[string]string, 0)
			for _, v := range strings.Split(param[1], ",") {
				if string(v[0]) == "-" {
					tmp = append(tmp, map[string]string{
						v[1:]: "desc",
					})
				} else {
					tmp = append(tmp, map[string]string{
						v: "asc",
					})
				}
			}
			tmpMap["sort"] = tmp
		default:
		}
	}

	result, err := json.Marshal(tmpMap)
	return string(result), err
}