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

type CalcAvgFormat struct{}

func (format CalcAvgFormat) Exec(args interface{}) func(data interface{}) (result interface{}, err error) {
	avgTitle := args.([]interface{})

	return func(data interface{}) (result interface{}, err error) {
		dataMap := data.([]map[string]interface{})

		for _, title := range avgTitle {
			title := title.(string)

			var sum float64
			var count float64
			for _, item := range dataMap {
				sum += any2float64(item[title])
				count += 1
			}
			var avg = sum / count

			key := "avg(" + title + ")"
			for _, item := range dataMap {
				item[key] = fmt.Sprintf("%.4f", avg)
			}
		}

		result = dataMap
		return
	}
}
