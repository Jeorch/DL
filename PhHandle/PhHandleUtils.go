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
package PhHandle

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
)

func extractUrlQuery(r *http.Request, queryObj interface{}) (err error) {
	queryString, err := url.QueryUnescape(r.URL.RawQuery)
	if err != nil {
		return
	}
	if "" == queryString {
		return
	}

	if err = json.Unmarshal([]byte(queryString), &queryObj); err != nil {
		return
	}
	return
}

func extractBodyQuery(r *http.Request, queryObj interface{}) (err error) {
	body, err := ioutil.ReadAll(r.Body)
	err = json.Unmarshal(body, &queryObj)
	return
}

func extractModel(r *http.Request, model interface{}) (err error) {
	err = extractBodyQuery(r, &model)
	err = extractUrlQuery(r, &model)

	return
}
