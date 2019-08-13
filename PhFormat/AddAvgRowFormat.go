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

import "fmt"

type AddAvgRowFormat struct{}

func (format AddAvgRowFormat) Exec(args interface{}) func(data interface{}) (result interface{}, err error) {
	keepTitle := args.([]interface{})

	return func(data interface{}) (result interface{}, err error) {
		dataMap := data.([]map[string]interface{})

		sumMap := make(map[string]float64)
		avgMap := make(map[string]float64)
		count := 0.0

		for _, item := range dataMap {
			for k, v := range item {
				if s, ok := sumMap[k]; ok {
					sumMap[k] = s + any2float64(v)
				} else {
					sumMap[k] = any2float64(v)
				}
			}
			count += 1
		}

		for k, v := range sumMap {
			avgMap[k] = v / count
		}

		tmp := make(map[string]interface{})
		for k, v := range avgMap {
			if sliceIndex(keepTitle, k) != -1 {
				tmp[k] = "平均值"
			} else {
				tmp[k] = fmt.Sprintf("%.4f", v)
			}
		}
		result = append(dataMap, tmp)
		return
	}
}

