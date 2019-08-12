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

func TestAddAvgRowFormat_Exec(t *testing.T) {
	data := []map[string]interface{}{
		{
			"firstname": "A",
			"lastname": "a",
			"age": 11,
		},
		{
			"firstname": "B",
			"lastname": "b",
			"age": 22,
		},
		{
			"firstname": "C",
			"lastname": "c",
			"age": 33,
		},
	}

	Convey("增加`平均`行", t, func() {
		keepTitle := []interface{}{"firstname", "lastname"}
		result, err := AddAvgRowFormat{}.Exec(keepTitle)(data)

		So(err, ShouldBeNil)
		So(result, ShouldNotBeNil)
		So(len(result.([]map[string]interface{})), ShouldEqual, 4)
	})
}
