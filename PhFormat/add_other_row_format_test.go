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
package PhFormat

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestAddOtherRowFormat_Exec(t *testing.T) {
	data := []map[string]interface{}{
		{
			"product": "A",
			"sales": 11,
			"potential": 100,
		},
		{
			"product": "B",
			"sales": 22,
			"potential": 100,
		},
		{
			"product": "C",
			"sales": 33,
			"potential": 100,
		},
	}

	Convey("增加`其他`行", t, func() {
		formula := map[string]interface{}{
			"fill": "其他",
			"keep": []interface{}{"potential"},
			"complete": "$potential",
			"value": "sales",
		}
		result, err := AddOtherRowFormat{}.Exec(formula)(data)

		for _, item := range result.([]map[string]interface{}) {
			fmt.Println(item)
		}
		So(err, ShouldBeNil)
		So(result, ShouldNotBeNil)
		So(len(result.([]map[string]interface{})), ShouldEqual, 4)
	})
}
