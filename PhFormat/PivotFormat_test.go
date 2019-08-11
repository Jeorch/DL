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
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestPivotFormat_Exec(t *testing.T) {
	data := []map[string]interface{}{
		{
			"firstname": "A",
			"lastname":  "a",
			"age":       11,
		},
		{
			"firstname": "A",
			"lastname":  "b",
			"age":       22,
		},
		{
			"firstname": "B",
			"lastname":  "a",
			"age":       33,
		},
		{
			"firstname": "B",
			"lastname":  "b",
			"age":       44,
		},
	}

	Convey("增加`比例`列", t, func() {
		pivot := map[string]interface{}{
			"yAxis": "firstname",
			"xAxis": "lastname",
			"value": "age",
		}
		result, err := PivotFormat{}.Exec(pivot)(data)

		So(err, ShouldBeNil)
		So(result, ShouldNotBeNil)
		So(len(result.([][]interface{})), ShouldEqual, 3)

		row := result.([][]interface{})[0]
		So(len(row), ShouldEqual, 3)
	})
}
