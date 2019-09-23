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
	"fmt"
	"strings"
)

type AddOtherRowFormat struct{}

func (format AddOtherRowFormat) Exec(args interface{}) func(data interface{}) (result interface{}, err error) {
	tmpArgs := args.(map[string]interface{})
	var fill string
	var keep []interface{}
	var complete string
	value := tmpArgs["value"].(string)

	if val, ok := tmpArgs["fill"]; ok {
		fill = val.(string)
	} else {
		fill = "其他"
	}

	if val, ok := tmpArgs["keep"]; ok {
		keep = val.([]interface{})
	} else {
		keep = []interface{}{""}
	}

	if val, ok := tmpArgs["complete"]; ok {
		switch val := val.(type) {
		case string:
			complete = val
		case float64:
			complete = fmt.Sprintf("%f", val)
		default:
			complete = "0.0"
		}
	} else {
		complete = "0.0"
	}

	return func(data interface{}) (result interface{}, err error) {
		dataMap := data.([]map[string]interface{})

		all := 0.0
		if strings.HasPrefix(complete, "$") {
			all = any2float64(dataMap[0][complete[1:]])
		} else {
			all = any2float64(complete)
		}

		for _, item := range dataMap {
			all -= any2float64(item[value])
		}

		tmp := make(map[string]interface{})
		for k, v := range dataMap[0] {
			if k == value {
				tmp[k] = all
			} else if sliceIndex(keep, k) > -1 {
				tmp[k] = v
			} else {
				tmp[k] = fill
			}
		}
		result = append(dataMap, tmp)
		return
	}
}

