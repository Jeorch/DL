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
	"fmt"
)

type AddColFormat struct{}

func (format AddColFormat) Exec(args interface{}) func(data interface{}) (result interface{}, err error) {
	addCols := args.([]interface{})

	return func(data interface{}) (interface{}, error) {
		dataMap := data.([]map[string]interface{})

		for _, addCol := range addCols {
			m := addCol.(map[string]interface{})
			name := m["name"].(string)

			// TODO：为了向前兼容
			switch m["value"].(type) {
			case string:
				m["value"] = []interface{}{"=", m["value"]}
			}

			for _, item := range dataMap {
				value, err := format.calcResult(m["value"], item)
				if err != nil {
					return nil, err
				}
				item[name] = value
			}
		}

		return dataMap, nil
	}
}

func (format AddColFormat) calcResult(expr interface{}, row map[string]interface{}) (interface{}, error) {
	switch exprType := expr.(type) {
	case []interface{}:
		switch oper := exprType[0].(string); oper {
		case "=":
			return exprType[1], nil
		case "$":
			return format.calcResult(exprType[1], row)
		case "+":
			left, right, err := format.doubleNumber(exprType, row)
			if err != nil {
				return nil, err
			}
			result := any2float64(left) + any2float64(right)
			return result, nil
		case "-":
			left, right, err := format.doubleNumber(exprType, row)
			if err != nil {
				return nil, err
			}
			result := any2float64(left) - any2float64(right)
			return result, nil
		case "*":
			left, right, err := format.doubleNumber(exprType, row)
			if err != nil {
				return nil, err
			}
			result := any2float64(left) * any2float64(right)
			return result, nil
		case "/":
			left, right, err := format.doubleNumber(exprType, row)
			if err != nil {
				return nil, err
			}
			if right == nil || any2float64(right) == 0 {
				return 0, nil
			}
			result := fmt.Sprintf("%.4f", any2float64(left)/any2float64(right))
			return result, nil
		default:
			return nil, errors.New(oper + "不支持的运算符")
		}
	default:
		value := row[exprType.(string)]
		if value == nil {
			return nil, errors.New("key 值不存在：" + exprType.(string))
		}
		return value, nil
	}
}

func (format AddColFormat) doubleNumber(exprSlice []interface{}, row map[string]interface{}) (interface{}, interface{}, error) {
	if len(exprSlice) != 3 {
		return nil, nil, errors.New("运算表达式不足3位")
	}
	left, err := format.calcResult(exprSlice[1], row)
	if err != nil {
		return nil, nil, err
	}
	right, err := format.calcResult(exprSlice[2], row)
	if err != nil {
		return nil, nil, err
	}
	return left, right, nil
}
