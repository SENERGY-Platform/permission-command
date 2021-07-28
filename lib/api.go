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
	"encoding/json"
	"github.com/SmartEnergyPlatform/permissions/lib/auth"
	"github.com/SmartEnergyPlatform/permissions/lib/util"
	"github.com/julienschmidt/httprouter"
	"log"
	"net/http"

	"strings"
)

func StartApi() {
	log.Println("connect to kafka: ", Config.KafkaUrl)
	InitEventConn()
	defer StopEventConn()
	log.Println("start server on port: ", Config.ServerPort)
	httpHandler := getRoutes()
	corseHandler := util.NewCors(httpHandler)
	logger := util.NewLogger(corseHandler, Config.LogLevel)
	log.Println(http.ListenAndServe(":"+Config.ServerPort, logger))
}

func getRoutes() (router *httprouter.Router) {
	router = httprouter.New()

	router.PUT("/user/:user/:resource_kind/:resource_id/:right", func(res http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		user := ps.ByName("user")
		kind := ps.ByName("resource_kind")
		right := ps.ByName("right")
		resource := ps.ByName("resource_id")
		token, err := auth.GetParsedToken(r)
		if err != nil {
			http.Error(res, err.Error(), http.StatusBadRequest)
			return
		}
		handleUserRightPut(res, user, kind, resource, right, token)
	})

	router.PUT("/user/:user/:resource_kind/:resource_id", func(res http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		user := ps.ByName("user")
		kind := ps.ByName("resource_kind")
		right := ""
		resource := ps.ByName("resource_id")
		token, err := auth.GetParsedToken(r)
		if err != nil {
			http.Error(res, err.Error(), http.StatusBadRequest)
			return
		}
		handleUserRightPut(res, user, kind, resource, right, token)
	})

	router.DELETE("/user/:user/:resource_kind/:resource_id", func(res http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		user := ps.ByName("user")
		kind := ps.ByName("resource_kind")
		resource := ps.ByName("resource_id")
		token, err := auth.GetParsedToken(r)
		if err != nil {
			http.Error(res, err.Error(), http.StatusBadRequest)
			return
		}
		if token.GetUserId() == user {
			log.Println("WARNING: user cant remove his own rights")
			http.Error(res, "user cant remove his own rights", http.StatusBadRequest)
			return
		}
		err = HasAdminRight(token.Token, kind, resource)
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
		json.NewEncoder(res).Encode(ok)
	})

	router.PUT("/group/:group/:resource_kind/:resource_id/:right", func(res http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		group := ps.ByName("group")
		kind := ps.ByName("resource_kind")
		right := ps.ByName("right")
		resource := ps.ByName("resource_id")
		token, err := auth.GetParsedToken(r)
		if err != nil {
			http.Error(res, err.Error(), http.StatusBadRequest)
			return
		}
		handleGroupRightPut(res, group, kind, resource, right, token)
	})

	router.PUT("/group/:group/:resource_kind/:resource_id", func(res http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		group := ps.ByName("group")
		kind := ps.ByName("resource_kind")
		right := ""
		resource := ps.ByName("resource_id")
		token, err := auth.GetParsedToken(r)
		if err != nil {
			http.Error(res, err.Error(), http.StatusBadRequest)
			return
		}
		handleGroupRightPut(res, group, kind, resource, right, token)
	})

	router.DELETE("/group/:group/:resource_kind/:resource_id", func(res http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		group := ps.ByName("group")
		kind := ps.ByName("resource_kind")
		resource := ps.ByName("resource_id")

		token, err := auth.GetParsedToken(r)
		if err != nil {
			http.Error(res, err.Error(), http.StatusBadRequest)
			return
		}

		// users may not remove admin from resource
		if group == "admin" && !token.IsAdmin() {
			http.Error(res, "only admin group may remove admin group from resource", http.StatusForbidden)
			return
		}

		err = HasAdminRight(token.Token, kind, resource)
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
		json.NewEncoder(res).Encode(ok)
	})

	return
}

func handleUserRightPut(res http.ResponseWriter, user string, kind string, resource string, right string, token auth.Token) {
	if token.GetUserId() == user && !strings.Contains(right, "a") {
		log.Println("WARNING: user cant remove own administration right")
		http.Error(res, "user cant remove own administration right", http.StatusBadRequest)
		return
	}
	err := HasAdminRight(token.Token, kind, resource)
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
	json.NewEncoder(res).Encode(ok)
}

func handleGroupRightPut(res http.ResponseWriter, group string, kind string, resource string, right string, token auth.Token) {
	// users may not remove admin from resource
	if group == "admin" && !token.IsAdmin() && !strings.Contains(right, "a") {
		http.Error(res, "only admin group may remove admin group from resource", http.StatusForbidden)
		return
	}
	err := HasAdminRight(token.Token, kind, resource)
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
	json.NewEncoder(res).Encode(ok)
}
