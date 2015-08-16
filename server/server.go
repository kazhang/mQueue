package main

import (
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	ID_LENGTH        = 10
	CLIENT_ID_LENGHT = 3
	LOG_NAME         = "log_000"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func logFatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func randString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func Encode(s string) string {
	s = strings.Replace(s, "|", "||", -1)
	return s
}

func Decode(s string) string {
	s = strings.Replace(s, "||", "|", -1)
	return s
}

type MsgServ struct {
	address        string
	dataPath       string
	consumers      map[string]net.Conn
	consumer_mutex sync.Mutex
	toMerge        chan string
}

func (ms *MsgServ) init() {
	ms.toMerge = make(chan string)
	ms.consumers = make(map[string]net.Conn)
}

func (ms *MsgServ) notify(data []byte) {
	for id, consumer := range ms.consumers {
		if _, err := consumer.Write(data); err != nil {
			log.Printf("connection %s broken\n", id)
			ms.consumer_mutex.Lock()
			delete(ms.consumers, id)
			ms.consumer_mutex.Unlock()
		}
	}
}

func (ms *MsgServ) Run() {
	ms.init()
	ln, err := net.Listen("tcp", ms.address)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	go ms.waitForMerge()
	for {
		conn, err := ln.Accept()
		logFatal(err)
		go ms.handler(conn)
	}
}

func (ms *MsgServ) recovery() {
}

func (ms *MsgServ) loadOffline() {
}

func (ms *MsgServ) merge(id string) {
	err := os.Rename(ms.dataPath+id, "current")
	logFatal(err)

	data, err := ioutil.ReadFile(ms.dataPath + "current")
	logFatal(err)
	dataS := "@|^" + Encode(string(data)) + "@|$"

	f, err := os.OpenFile(ms.dataPath+LOG_NAME, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	logFatal(err)

	_, err = f.WriteString(dataS)
	logFatal(err)
	ms.notify(data)

	f.Close()
	err = os.Remove(ms.dataPath + "current")
	logFatal(err)
}

func (ms *MsgServ) waitForMerge() {
	for {
		id := <-ms.toMerge
		go ms.merge(id)
	}
}

func (ms *MsgServ) handler(conn net.Conn) {
	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	logFatal(err)
	if buf[0] == 'w' {
		ms.handleInjection(conn)
	} else if buf[0] == 'r' {
		ms.handleSubscription(conn)
	} else {
		_, err = conn.Write([]byte("bad request"))
		logFatal(err)
		conn.Close()
	}
}

func (ms *MsgServ) handleSubscription(conn net.Conn) {
	ms.consumer_mutex.Lock()
	id := randString(CLIENT_ID_LENGHT)
	if _, pre := ms.consumers[id]; pre != false {
		id = randString(CLIENT_ID_LENGHT)
	}
	log.Printf("add connectin %s\n", id)
	ms.consumers[id] = conn
	ms.consumer_mutex.Unlock()
}

func (ms *MsgServ) handleInjection(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 4096)
	_, err := conn.Read(buf)
	logFatal(err)

	id := randString(ID_LENGTH)
	tmpId := "." + id

	err = ioutil.WriteFile(ms.dataPath+tmpId, buf, 0644)
	logFatal(err)

	err = os.Rename(ms.dataPath+tmpId, ms.dataPath+id)
	logFatal(err)

	ms.toMerge <- id

	_, err = conn.Write([]byte("ack"))
	logFatal(err)
}

func (ms *MsgServ) Close() {
	ms.consumer_mutex.Lock()
	for _, conn := range ms.consumers {
		conn.Close()
	}
	// Going to exit, no need to unlock
}

func main() {
	ms := MsgServ{address: ":8181", dataPath: "./"}
	ms.Run()
	ms.Close()
}
