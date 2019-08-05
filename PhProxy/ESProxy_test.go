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
package PhProxy

import (
	"fmt"
	. "github.com/PharbersDeveloper/DL/PhModel"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

var tESHost = "192.168.100.157"
var tESPort = "9200"

func TestESProxy_NewProxy(t *testing.T) {
	proxy := ESProxy{}.NewProxy(map[string]string{
		"host": tESHost,
		"port": tESPort,
	})

	esClient := proxy.esClient

	Convey("Test create ES Proxy", t, func() {
		So(proxy, ShouldNotBeNil)
		So(esClient, ShouldNotBeNil)
	})
}

func TestESProxy_Create(t *testing.T) {
	proxy := ESProxy{}.NewProxy(map[string]string{
		"host": tESHost,
		"port": tESPort,
	})

	model := PhModel {
		Model: "test",
		Insert: []map[string]interface{}{
			{
				"firstname": "A",
				"lastname":  "a",
				"age":       11,
			},
			{
				"firstname": "B",
				"lastname":  "b",
				"age":       22,
			},
			{
				"firstname": "C",
				"lastname":  "c",
				"age":       33,
			},
		},
	}

	Convey("Test ES search all index", t, func() {
		err := proxy.Create(model)

		So(err, ShouldBeNil)
	})
}

func TestESProxy_Read(t *testing.T) {
	proxy := ESProxy{}.NewProxy(map[string]string{
		"host": tESHost,
		"port": tESPort,
	})

	fmt.Println(proxy.port)

	//model := PhModel {
	//	Model: "test",
	//	Format:[]map[string]interface{}{
	//		{
	//			"class": "cut2DArray",
	//			"args":  []string{"firstname", "age"},
	//		},
	//	},
	//}
	//
	//Convey("Test ES search all index", t, func() {
	//	result, err := es.Read(model)
	//	fmt.Println(result)
	//
	//	So(err, ShouldBeNil)
	//	So(result, ShouldNotBeNil)
	//})
}
