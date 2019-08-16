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
	"log"
)

func any2float64(any interface{}) float64 {
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

func sliceIndex(slice []interface{}, item interface{}) int {
	for i, val := range slice {
		if val == item {
			return i
		}
	}
	return -1
}

func sliceReverse(slice []interface{}) {
	for i := len(slice)/2 - 1; i >= 0; i-- {
		opp := len(slice) - 1 - i
		slice[i], slice[opp] = slice[opp], slice[i]
	}
	return
}

func sliceBubbleSort(slice []interface{}) {
	compare := func(prev, next interface{}) bool {
		switch t := prev.(type) {
		case int:
			return t > next.(int)
		case int64:
			return t > next.(int64)
		case float32:
			return t > next.(float32)
		case float64:
			return t > next.(float64)
		case string:
			return t > next.(string)
		default:
			log.Fatal(fmt.Sprint("%v, %v not support compare", prev, next))
			return true
		}
	}

	for i := 0; i < len(slice); i++ {
		for j := 1; j < len(slice)-i; j++ {
			if compare(slice[j-1], slice[j]) {
				slice[j], slice[j-1] = slice[j-1], slice[j]
			}
		}
	}
}

func sliceQuickSortByString(slice []interface{}) {
	recursionSort := func(nums []interface{}, left int, right int) {}

	recursionSort = func(nums []interface{}, left int, right int) {
		partition := func(nums []interface{}, left int, right int) int {
			for left < right {
				for left < right && nums[left].(string) <= nums[right].(string) {
					right--
				}
				if left < right {
					nums[left], nums[right] = nums[right], nums[left]
					left++
				}

				for left < right && nums[left].(string) <= nums[right].(string) {
					left++
				}
				if left < right {
					nums[left], nums[right] = nums[right], nums[left]
					right--
				}
			}

			return left
		}

		if left < right {
			pivot := partition(nums, left, right)
			recursionSort(nums, left, pivot-1)
			recursionSort(nums, pivot+1, right)
		}
	}

	recursionSort(slice, 0, len(slice)-1)
}
