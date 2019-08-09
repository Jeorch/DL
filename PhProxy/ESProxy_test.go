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
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

const (
	tESHost = "127.0.0.1"
	tESPort = "9200"
	table   = "es-test"
)

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

	insert := []map[string]interface{}{
		{
			"firstname": "张",
			"lastname":  "三",
			"age":       11,
		},
		{
			"firstname": "李",
			"lastname":  "四",
			"age":       22,
		},
		{
			"firstname": "王",
			"lastname":  "五",
			"age":       33,
		},
		{
			"firstname": "张",
			"lastname":  "全蛋",
			"age":       111,
		},
		{
			"firstname": "张",
			"lastname":  "少蛋",
			"age":       222,
		},
		{
			"firstname": "张",
			"lastname":  "蛋大",
			"age":       333,
		},
	}

	Convey("Test ES insert multi data", t, func() {
		result, err := proxy.Create(table, insert)

		So(result, ShouldNotBeEmpty)
		So(err, ShouldBeNil)
	})
}

func TestESProxy_Update(t *testing.T) {
	t.SkipNow()
}

func TestESProxy_Read(t *testing.T) {
	proxy := ESProxy{}.NewProxy(map[string]string{
		"host": tESHost,
		"port": tESPort,
	})

	Convey("查询全部文档", t, func() {

		query := map[string]interface{}{
			"search": nil,
		}
		result, err := proxy.Read([]string{table}, query)

		So(err, ShouldBeNil)
		So(result, ShouldNotBeNil)
		So(len(result), ShouldEqual, 6)
	})

	Convey("查询全部文档并递减排序", t, func() {
		query := map[string]interface{}{
			"search": map[string]interface{}{
				"sort" : []interface{}{"-age"},
			},
		}
		result, err := proxy.Read([]string{table}, query)

		So(err, ShouldBeNil)
		So(result, ShouldNotBeNil)
		So(len(result), ShouldEqual, 6)
		So(result[0]["age"], ShouldBeGreaterThan, result[1]["age"])
	})

	Convey("简单条件查询", t, func() {
		query := map[string]interface{}{
			"search": map[string]interface{}{
				"sort" : []interface{}{"-age"},
				"and": []interface{}{
					[]interface{}{"eq", "firstname.keyword", "张"},
					[]interface{}{"lte", "age", 300},
					[]interface{}{"gte", "age", 100},
				},
			},
		}
		result, err := proxy.Read([]string{table}, query)

		So(err, ShouldBeNil)
		So(result, ShouldNotBeNil)
		So(len(result), ShouldEqual, 2)
		So(result[0]["age"], ShouldBeGreaterThan, result[1]["age"])
		So(result[0]["firstname"], ShouldEqual, "张")
		So(result[1]["firstname"], ShouldEqual, "张")
	})

	Convey("嵌套查询", t, func() {
		query := map[string]interface{}{
			"search": map[string]interface{}{
				"sort": []interface{}{"-age"},
				"or": []interface{}{
					[]interface{}{"and", []interface{}{
						[]interface{}{"eq", "firstname.keyword", "张"},
						[]interface{}{"lte", "age", 300},
						[]interface{}{"gte", "age", 100},
					}},
					[]interface{}{"eq", "firstname.keyword", "李"},
				},
			},
		}
		result, err := proxy.Read([]string{table}, query)

		So(err, ShouldBeNil)
		So(result, ShouldNotBeNil)
		So(len(result), ShouldEqual, 3)
		So(result[0]["age"], ShouldBeGreaterThan, result[1]["age"])
		So(result[0]["firstname"], ShouldEqual, "张")
		So(result[1]["firstname"], ShouldEqual, "张")
		So(result[2]["firstname"], ShouldEqual, "李")
	})

	Convey("桶聚合", t, func() {
		query := map[string]interface{}{
			"aggs": []interface{}{
				map[string]interface{}{
					"groupBy": "firstname.keyword",
					"aggs": []interface{}{
						map[string]interface{}{
							"groupBy": "lastname.keyword",
							"aggs": []interface{}{
								map[string]interface{}{"agg": "sum", "field": "age"},
								map[string]interface{}{"agg": "avg", "field": "age"},
							},
						},
						map[string]interface{}{"agg": "sum", "field": "age"},
						map[string]interface{}{"agg": "avg", "field": "age"},
					},
				},
			},
		}
		result, err := proxy.Read([]string{table}, query)

		So(err, ShouldBeNil)
		So(result, ShouldNotBeNil)
		So(len(result), ShouldEqual, 9)
	})

	Convey("查询并桶聚合", t, func() {
		query := map[string]interface{}{
			"search": map[string]interface{}{
				"and": []interface{}{
					[]interface{}{"lte", "age", 300},
				},
			},
			"aggs": []interface{}{
				map[string]interface{}{
					"groupBy": "firstname.keyword",
					"aggs": []interface{}{
						map[string]interface{}{
							"groupBy": "lastname.keyword",
							"aggs": []interface{}{
								map[string]interface{}{"agg": "sum", "field": "age"},
								map[string]interface{}{"agg": "avg", "field": "age"},
							},
						},
						map[string]interface{}{"agg": "sum", "field": "age"},
						map[string]interface{}{"agg": "avg", "field": "age"},
					},
				},
			},
		}
		result, err := proxy.Read([]string{table}, query)

		So(err, ShouldBeNil)
		So(result, ShouldNotBeNil)
		So(len(result), ShouldEqual, 8)
	})
}

func TestESProxy_Delete(t *testing.T) {
	t.SkipNow()
}
