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

type PivotFormat struct{}

func (format PivotFormat) Exec(args interface{}) func(data interface{}) (result interface{}, err error) {
	tmpArgs := args.(map[string]interface{})
	yAxis := tmpArgs["yAxis"].(string)
	xAxis := tmpArgs["xAxis"].(string)
	value := tmpArgs["value"].(string)
	var reverse bool
	if ok := tmpArgs["reverse"]; ok != nil {
		reverse = ok.(bool)
	}

	return func(data interface{}) (result interface{}, err error) {
		dataMap := data.([]map[string]interface{})
		tmpResult := make([][]interface{}, 0)

		// 提取Y轴和X轴
		ySlice := format.extractAxis(dataMap, yAxis)
		xSlice := format.extractAxis(dataMap, xAxis)
		if len(xSlice) == 0 {
			err = errors.New("X 轴错误，key不存在")
			return
		}
		if len(ySlice) == 0 {
			err = errors.New("Y 轴错误，key不存在")
			return
		}

		// 写入内容
		for _, y := range ySlice {
			arr := make([]interface{}, 0)
			arr = append(arr, y)
			for _, x := range xSlice {
				var tmp interface{}
				for _, item := range dataMap {
					if item[yAxis] == y && item[xAxis] == x {
						tmp = item[value]
						break
					}
				}
				arr = append(arr, tmp)
			}
			tmpResult = append(tmpResult, arr)
		}

		// 是否反转数组
		if reverse {
			for i := len(tmpResult)/2 - 1; i >= 0; i-- {
				opp := len(tmpResult) - 1 - i
				tmpResult[i], tmpResult[opp] = tmpResult[opp], tmpResult[i]
			}
		}

		head := append([]interface{}{value}, xSlice...)
		tmpResult = append([][]interface{}{head}, tmpResult...)

		result = tmpResult
		return
	}
}

func (format PivotFormat) extractAxis(data []map[string]interface{}, key string) []interface{} {
	arr := make([]interface{}, 0)
	for _, item := range data {
		if format.exist(arr, item[key]) {
			continue
		}
		if v, ok := item[key]; ok {
			arr = append(arr, v)
		}
	}

	return arr
}

func (format PivotFormat) exist(s []interface{}, e interface{}) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
