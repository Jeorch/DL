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

type FilterFormat struct{}

func (format FilterFormat) Exec(args interface{}) func(data interface{}) (result interface{}, err error) {
	filterConds := args.([]interface{})
	condFunc := format.genCondFunc(filterConds)

	return func(data interface{}) (result interface{}, err error) {
		dataMap := data.([]map[string]interface{})
		tmpMap := make([]map[string]interface{}, 0)

		for _, item := range dataMap {
			if condFunc(item) {
				tmpMap = append(tmpMap, item)
			}
		}

		return tmpMap, nil
	}
}

func (format FilterFormat) genCondFunc(conds []interface{}) func(map[string]interface{}) bool {
	cond := conds[0].([]interface{})
	return func(data map[string]interface{}) bool {
		b := false
		switch cond[0].(string) {
		case "and":
			for _, sub := range cond[1].([]interface{}) {
				sub := sub.([]interface{})
				if sub[0].(string) == "eq" && data[sub[1].(string)] == sub[2] {
					b = true
				} else {
					b = false
					break
				}
			}
			break
		case "or":
			for _, sub := range cond[1].([]interface{}) {
				sub := sub.([]interface{})
				if sub[0].(string) == "eq" && data[sub[1].(string)] == sub[2] {
					b = true
					break
				} else {
					b = false
				}
			}
		}
		return b
	}
}
