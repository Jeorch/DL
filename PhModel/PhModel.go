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
package PhModel

import (
	"github.com/PharbersDeveloper/DL/PhFactory"
	"github.com/alfredyang1986/blackmirror/bmlog"
	"log"
	"reflect"
)

var table = PhFactory.PhTable{}

type PhModel struct {
	Model  string
	Query  map[string]interface{}
	Insert []map[string]interface{}
	Update []map[string]interface{}
	Format []map[string]interface{}
}

func (m PhModel) IsEmpty() bool {
	return reflect.DeepEqual(m, PhModel{})
}

func (m PhModel) FormatResult(data interface{}) (result interface{}, err error) {
	result = data
	for _, plugin := range m.Format {
		bmlog.StandardLogger().Infof("开始格式化 class='%s', args='%#v'", plugin["class"].(string), plugin["args"])
		log.Printf("开始格式化 class='%s', args='%#v'", plugin["class"].(string), plugin["args"])
		if class := table.GetFormat(plugin["class"].(string)); class != nil {
			result, err = class.Exec(plugin["args"])(result)
			if err != nil {
				return
			}
		} else {
			bmlog.StandardLogger().Error("不存在Format：" + plugin["class"].(string))
			log.Println("不存在Format：" + plugin["class"].(string))
		}
	}
	return
}
