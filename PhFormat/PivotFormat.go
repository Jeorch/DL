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
	head := value
	if val, ok := tmpArgs["head"]; ok {
		head = val.(string)
	}

	return func(data interface{}) (result interface{}, err error) {
		dataMap := data.([]map[string]interface{})

		// 提取Y轴和X轴，并排序
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

		// 初始化二维数组
		tmpResult := make([]interface{}, len(ySlice))
		for i := 0; i < len(ySlice); i++ {
			tmpX := make([]interface{}, len(xSlice)+1)
			tmpX[0] = ySlice[i]
			tmpResult[i] = tmpX
		}

		// 写入内容
		for _, item := range dataMap {
			y := sliceIndex(ySlice, item[yAxis])
			x := sliceIndex(xSlice, item[xAxis])
			if y > -1 && x > -1 {
				tmpY := tmpResult[y].([]interface{})
				tmpY[x+1] = item[value]
			}
		}

		// 写入表头
		firstRow := append([]interface{}{head}, xSlice...)
		tmpResult = append([]interface{}{firstRow}, tmpResult...)

		result = tmpResult
		return
	}
}

func (format PivotFormat) extractAxis(data []map[string]interface{}, key string) ([]interface{}, string) {
	sort := ""
	if strings.HasPrefix(key, "-") {
		sort = "desc"
		key = key[1:]
	} else if strings.HasPrefix(key, "+") {
		sort = "asc"
		key = key[1:]
	}

	arr := make([]interface{}, 0)
	// 提取
	for _, item := range data {
		if sliceIndex(arr, item[key]) != -1 {
			continue
		}
		if v, ok := item[key]; ok {
			arr = append(arr, v)
		}
	}

	switch sort {
	case "desc":
		sliceBubbleSort(arr)
		sliceReverse(arr)
	case "asc":
		sliceBubbleSort(arr)
	default:
	}

	return arr, key
}
