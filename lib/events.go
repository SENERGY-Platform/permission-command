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
)

var conn *Publisher

type PermCommandMsg struct {
	Command  string `json:"command"`
	Kind     string
	Resource string
	User     string
	Group    string
	Right    string
}

func InitEventConn() {
	var err error
	conn, err = NewPublisher()
	if err != nil {
		log.Fatal("ERROR: while initializing amqp connection", err)
	}
}

func StopEventConn() {
	conn.writer.Close()
}

func sendEvent(command PermCommandMsg) error {
	return conn.Publish(command)
}

func SetGroupRight(kind, resource, group, right string) error {
	return sendEvent(PermCommandMsg{
		Command:  "PUT",
		Kind:     kind,
		Resource: resource,
		Group:    group,
		Right:    right,
	})
}

func DeleteGroupRight(kind, resource, group string) error {
	return sendEvent(PermCommandMsg{
		Command:  "DELETE",
		Kind:     kind,
		Resource: resource,
		Group:    group,
	})
}

func DeleteUserRight(kind, resource, user string) error {
	return sendEvent(PermCommandMsg{
		Command:  "DELETE",
		Kind:     kind,
		Resource: resource,
		User:     user,
	})
}

func SetUserRight(kind, resource, user, right string) error {
	return sendEvent(PermCommandMsg{
		Command:  "PUT",
		Kind:     kind,
		Resource: resource,
		User:     user,
		Right:    right,
	})
}
