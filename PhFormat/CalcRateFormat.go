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

type CalcRateFormat struct{}

func (format CalcRateFormat) Exec(args interface{}) func(data interface{}) (result interface{}, err error) {
	rateTitle := args.([]interface{})

	any2float64 := func(any interface{}) float64 {
		switch t := any.(type) {
		case int:
			return float64(t)
		case int64:
			return float64(t)
		case float32:
			return float64(t)
		case float64:
			return t
		default:
			return 0
		}
	}

	return func(data interface{}) (result interface{}, err error) {
		dataMap := data.([]map[string]interface{})

		for _, title := range rateTitle {
			title := title.(string)

			var sum float64
			for _, item := range dataMap {
				sum += any2float64(item[title])
			}

			key := "rate(" + title + ")"
			for _, item := range dataMap {
				if sum == 0 {
					item[key] = 0
				} else {
					item[key] = fmt.Sprintf("%.4f", any2float64(item[title]) / sum)
				}
			}
		}

		result = dataMap
		return
	}
}
