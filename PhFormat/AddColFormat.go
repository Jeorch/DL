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

type AddColFormat struct{}

func (format AddColFormat) Exec(args interface{}) func(data interface{}) (result interface{}, err error) {
	addCols := args.([]interface{})

	return func(data interface{}) (result interface{}, err error) {
		dataMap := data.([]map[string]interface{})

		for _, addCol := range addCols {
			m := addCol.(map[string]interface {})
			name := m["name"].(string)
			value := m["value"]

			for _, item := range dataMap {
				item[name] = value
			}
		}

		result = dataMap
		return
	}
}
