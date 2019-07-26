package PhProxy

import (
	"net/http"
	"io/ioutil"
	"encoding/json"
	"fmt"
	"io"
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
	//reqMethod := "POST"
	//reqUrl := fmt.Sprintf("%s://%s:%s/%s/_doc?pretty",
	//	proxy.Protocol,
	//	proxy.Host,
	//	proxy.Port,
	//	args["model"],
	//)
	//reqBody := ioutil.NopCloser(strings.NewReader(args["cond"]))
	//
	//return proxy.callHttp(reqMethod, reqUrl, reqBody)
	return
}

func (proxy ESProxy) Update(args map[string]interface{}) (data map[string]interface{}, err error) {
	//reqMethod := "POST"
	//reqUrl := fmt.Sprintf("%s://%s:%s/%s/_doc?pretty",
	//	proxy.Protocol,
	//	proxy.Host,
	//	proxy.Port,
	//	args["model"],
	//)
	//reqBody := ioutil.NopCloser(strings.NewReader(args["cond"]))
	//
	//return proxy.callHttp(reqMethod, reqUrl, reqBody)
	return
}

func (proxy ESProxy) Read(args map[string]interface{}) (data map[string]interface{}, err error) {
	reqMethod := "GET"
	reqUrl := fmt.Sprintf("%s://%s:%s/%s/_doc/_search",
		proxy.Protocol,
		proxy.Host,
		proxy.Port,
		args["model"],
	)
	var reqBody io.ReadCloser

	if cond, ok := args["cond"]; ok {
		reqBody = ioutil.NopCloser(strings.NewReader(cond.(string)))
	}

	return proxy.callHttp(reqMethod, reqUrl, reqBody)
}

func (proxy ESProxy) Delete(args map[string]interface{}) (data map[string]interface{}, err error) {
	//reqMethod := "DELETE"
	//reqUrl := fmt.Sprintf("%s://%s:%s/%s/_doc?pretty",
	//	proxy.Protocol,
	//	proxy.Host,
	//	proxy.Port,
	//	args["model"],
	//)
	//reqBody := ioutil.NopCloser(strings.NewReader(args["cond"]))
	//
	//return proxy.callHttp(reqMethod, reqUrl, reqBody)
	return
}

func (proxy ESProxy) Format(data map[string]interface{}) (resp interface{}, err error) {
	root := make([][]interface{}, 0)

	title := data["title"].([]string)
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

func (proxy ESProxy) callHttp(method, url string, body io.Reader) (data map[string]interface{}, err error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return
	}

	req.Header.Add("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer response.Body.Close()

	respBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}
	json.Unmarshal(respBody, &data)

	return
}
