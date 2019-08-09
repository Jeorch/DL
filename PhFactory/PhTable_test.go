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
package PhFactory

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestPhTable_GetFormat(t *testing.T) {
	name := "cut2DArray"
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

	Convey("Test exec format by name", t, func() {
		result, err := PhTable{}.GetFormat(name).Exec([]interface{}{"firstname", "age"})(data)

		So(err, ShouldBeNil)
		So(result, ShouldNotBeNil)
	})
}
