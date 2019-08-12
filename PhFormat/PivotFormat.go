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
	"errors"
	"strings"
)

type PivotFormat struct{}

func (format PivotFormat) Exec(args interface{}) func(data interface{}) (result interface{}, err error) {
	tmpArgs := args.(map[string]interface{})
	yAxis := tmpArgs["yAxis"].(string)
	xAxis := tmpArgs["xAxis"].(string)
	value := tmpArgs["value"].(string)

	return func(data interface{}) (result interface{}, err error) {
		dataMap := data.([]map[string]interface{})
		tmpResult := make([]interface{}, 0)

		// 提取Y轴和X轴
		ySlice, yAxis := format.extractAxis(dataMap, yAxis)
		xSlice, xAxis := format.extractAxis(dataMap, xAxis)
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

		head := append([]interface{}{value}, xSlice...)
		tmpResult = append([]interface{}{head}, tmpResult...)

		result = tmpResult
		return
	}
}

func (format PivotFormat) extractAxis(data []map[string]interface{}, key string) ([]interface{}, string) {
	var reverse bool
	if strings.HasPrefix(key, "-") {
		reverse = true
		key = key[1:]
	}

	arr := make([]interface{}, 0)
	// 提取
	for _, item := range data {
		if sliceExist(arr, item[key]) {
			continue
		}
		if v, ok := item[key]; ok {
			arr = append(arr, v)
		}
	}

	// 排序
	sorted := func(slice []interface{}) []interface{}{
		for i := 0; i < len(slice); i++ {
			for j := 1; j < len(slice)-i; j++ {
				if slice[j].(string) < slice[j-1].(string) {
					slice[j], slice[j-1] = slice[j-1], slice[j]
				}
			}
		}
		return slice
	}(arr)

	if reverse {
		sliceReverse(sorted)
	}

	return sorted, key
}

