package main

import (
	bitcask "bitcask-go"
	bitcaskredis "bitcask-go/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const addr = "127.0.0.1:6380"

type BitcaskServer struct {
	dbs    map[int]*bitcaskredis.DataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	redisDataStructure, err := bitcaskredis.NewDataStructure(bitcask.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化 bitcask server
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcaskredis.DataStructure),
	}
	bitcaskServer.dbs[0] = redisDataStructure

	// 初始化 redis 服务器
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)

	bitcaskServer.listen()
}

func (b *BitcaskServer) listen() {
	log.Printf("redis server listening on %s\n", addr)
	_ = b.server.ListenAndServe()
}

func (b *BitcaskServer) accept(conn redcon.Conn) bool {
	client := new(BitcaskClient)
	b.mu.Lock()
	defer b.mu.Unlock()
	client.server = b
	client.db = b.dbs[0]

	conn.SetContext(client)
	return true
}

func (b *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range b.dbs {
		_ = db.Close()
	}
	_ = b.server.Close()
}
