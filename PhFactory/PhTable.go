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
package PhFactory

import "github.com/PharbersDeveloper/DL/PhFormat"

var phFormatFactory = map[string]PhFormat.PhFormat{
	"filter":      PhFormat.FilterFormat{},
	"cut2DArray":  PhFormat.Cut2DArrayFormat{},
	"calcRate":    PhFormat.CalcRateFormat{},
	"calcAvg":     PhFormat.CalcAvgFormat{},
	"addCol":      PhFormat.AddColFormat{},
	"addAvgRow":   PhFormat.AddAvgRowFormat{},
	"addOtherRow": PhFormat.AddOtherRowFormat{},
	"pivot":       PhFormat.PivotFormat{},
}

type PhTable struct{}

func (t PhTable) GetFormat(name string) PhFormat.PhFormat {
	return phFormatFactory[name]
}
