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

import "errors"

type Cut2DArrayFormat struct{}

func (format Cut2DArrayFormat) Exec(args interface{}) func(data interface{}) (result interface{}, err error) {
	keepTitle := args.([]interface{})

	return func(data interface{}) (result interface{}, err error) {
		tmpResult := make([][]interface{}, 0)
		if len(keepTitle) == 0 {
			err = errors.New("未设置要保留的2D数组表头")
			return
		} else {
			arr := make([]interface{}, 0)
			for _, i := range keepTitle {
				arr = append(arr, i)
			}
			tmpResult = append(tmpResult, arr)
		}

		for _, item := range data.([]map[string]interface{}) {
			arr := make([]interface{}, 0)
			for _, key := range keepTitle {
				arr = append(arr, item[key.(string)])
			}
			tmpResult = append(tmpResult, arr)
		}

		result = tmpResult
		return
	}
}
