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
	"bytes"
	. "github.com/smartystreets/goconvey/convey"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
)

func TestExtractUrlQuery(t *testing.T) {
	r := &http.Request {
		URL: &url.URL {
			RawQuery: url.QueryEscape("{\"key2\":\"value2\"}"),
		},
		Body: ioutil.NopCloser(bytes.NewReader([]byte(`{"key1":"value1"}`))),
	}

	Convey("Test extract url query", t, func() {
		var obj map[string]interface{}
		err := extractUrlQuery(r, &obj)

		So(err, ShouldBeNil)
		So(obj, ShouldNotBeNil)
	})

	Convey("Test extract body query", t, func() {
		var obj map[string]interface{}
		err := extractBodyQuery(r, &obj)

		So(err, ShouldBeNil)
		So(obj, ShouldNotBeNil)
	})

	Convey("Test extract model", t, func() {
		var obj map[string]interface{}
		err := extractModel(r, &obj)

		So(err, ShouldBeNil)
		So(obj, ShouldNotBeNil)
	})
}
