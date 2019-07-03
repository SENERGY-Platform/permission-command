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
	"log"
	"net/http"

	"strings"

	"github.com/SmartEnergyPlatform/jwt-http-router"
	"github.com/SmartEnergyPlatform/util/http/cors"
	"github.com/SmartEnergyPlatform/util/http/logger"
	"github.com/SmartEnergyPlatform/util/http/response"
)

func StartApi() {
	log.Println("connect to kafka: ", Config.ZookeeperUrl)
	InitEventConn()
	defer StopEventConn()
	log.Println("start server on port: ", Config.ServerPort)
	httpHandler := getRoutes()
	corseHandler := cors.New(httpHandler)
	logger := logger.New(corseHandler, Config.LogLevel)
	log.Println(http.ListenAndServe(":"+Config.ServerPort, logger))
}

func getRoutes() (router *jwt_http_router.Router) {
	router = jwt_http_router.New(jwt_http_router.JwtConfig{
		ForceUser: Config.ForceUser == "true",
		ForceAuth: Config.ForceAuth == "true",
		PubRsa:    Config.JwtPubRsa,
	})

	router.PUT("/user/:user/:resource_kind/:resource_id/:right", func(res http.ResponseWriter, r *http.Request, ps jwt_http_router.Params, jwt jwt_http_router.Jwt) {
		user := ps.ByName("user")
		kind := ps.ByName("resource_kind")
		right := ps.ByName("right")
		resource := ps.ByName("resource_id")
		handleUserRightPut(res, user, kind, resource, right, jwt)
	})

	router.PUT("/user/:user/:resource_kind/:resource_id", func(res http.ResponseWriter, r *http.Request, ps jwt_http_router.Params, jwt jwt_http_router.Jwt) {
		user := ps.ByName("user")
		kind := ps.ByName("resource_kind")
		right := ""
		resource := ps.ByName("resource_id")
		handleUserRightPut(res, user, kind, resource, right, jwt)
	})

	router.DELETE("/user/:user/:resource_kind/:resource_id", func(res http.ResponseWriter, r *http.Request, ps jwt_http_router.Params, jwt jwt_http_router.Jwt) {
		user := ps.ByName("user")
		kind := ps.ByName("resource_kind")
		resource := ps.ByName("resource_id")
		if jwt.UserId == user {
			log.Println("WARNING: user cant remove his own rights")
			http.Error(res, "user cant remove his own rights", http.StatusBadRequest)
			return
		}
		err := HasAdminRight(jwt.Impersonate, kind, resource)
		if err != nil {
			http.Error(res, err.Error(), http.StatusUnauthorized)
			return
		}
		err = DeleteUserRight(kind, resource, user)
		if err != nil {
			log.Println("ERROR", err)
			http.Error(res, err.Error(), http.StatusInternalServerError)
			return
		}
		ok := map[string]string{"status": "ok"}
		response.To(res).Json(ok)
	})

	router.PUT("/group/:group/:resource_kind/:resource_id/:right", func(res http.ResponseWriter, r *http.Request, ps jwt_http_router.Params, jwt jwt_http_router.Jwt) {
		group := ps.ByName("group")
		kind := ps.ByName("resource_kind")
		right := ps.ByName("right")
		resource := ps.ByName("resource_id")
		handleGroupRightPut(res, group, kind, resource, right, jwt)
	})

	router.PUT("/group/:group/:resource_kind/:resource_id", func(res http.ResponseWriter, r *http.Request, ps jwt_http_router.Params, jwt jwt_http_router.Jwt) {
		group := ps.ByName("group")
		kind := ps.ByName("resource_kind")
		right := ""
		resource := ps.ByName("resource_id")
		handleGroupRightPut(res, group, kind, resource, right, jwt)
	})

	router.DELETE("/group/:group/:resource_kind/:resource_id", func(res http.ResponseWriter, r *http.Request, ps jwt_http_router.Params, jwt jwt_http_router.Jwt) {
		group := ps.ByName("group")
		kind := ps.ByName("resource_kind")
		resource := ps.ByName("resource_id")
		err := HasAdminRight(jwt.Impersonate, kind, resource)
		if err != nil {
			http.Error(res, err.Error(), http.StatusUnauthorized)
			return
		}
		err = DeleteGroupRight(kind, resource, group)
		if err != nil {
			log.Println("ERROR", err)
			http.Error(res, err.Error(), http.StatusInternalServerError)
			return
		}
		ok := map[string]string{"status": "ok"}
		response.To(res).Json(ok)
	})

	return
}

func handleUserRightPut(res http.ResponseWriter, user string, kind string, resource string, right string, jwt jwt_http_router.Jwt) {
	if jwt.UserId == user && !strings.Contains(right, "a") {
		log.Println("WARNING: user cant remove own administration right")
		http.Error(res, "user cant remove own administration right", http.StatusBadRequest)
		return
	}
	err := HasAdminRight(jwt.Impersonate, kind, resource)
	if err != nil {
		http.Error(res, err.Error(), http.StatusUnauthorized)
		return
	}
	err = SetUserRight(kind, resource, user, right)
	if err != nil {
		log.Println("ERROR", err)
		http.Error(res, err.Error(), http.StatusInternalServerError)
		return
	}
	ok := map[string]string{"status": "ok"}
	response.To(res).Json(ok)
}

func handleGroupRightPut(res http.ResponseWriter, group string, kind string, resource string, right string, jwt jwt_http_router.Jwt) {
	err := HasAdminRight(jwt.Impersonate, kind, resource)
	if err != nil {
		http.Error(res, err.Error(), http.StatusUnauthorized)
		return
	}
	err = SetGroupRight(kind, resource, group, right)
	if err != nil {
		log.Println("ERROR", err)
		http.Error(res, err.Error(), http.StatusInternalServerError)
		return
	}
	ok := map[string]string{"status": "ok"}
	response.To(res).Json(ok)
}
