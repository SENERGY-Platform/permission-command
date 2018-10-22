/*
 * Copyright 2018 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package lib

import (
	"net/http"

	"net/url"

	"errors"

	"github.com/SmartEnergyPlatform/jwt-http-router"
)

func HasAdminRight(impersonate jwt_http_router.JwtImpersonate, kind string, id string) error {
	if Config.PermissionsViewUrl == "" {
		return nil
	}
	resp, err := impersonate.Get(Config.PermissionsViewUrl + "/jwt/check/" + url.QueryEscape(kind) + "/" + url.QueryEscape(id) + "/a")
	if err == nil && resp.StatusCode != http.StatusOK {
		err = errors.New("access denied")
	}
	return err
}
