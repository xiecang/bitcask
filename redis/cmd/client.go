package main

import (
	bitcask "bitcask-go"
	bitcaskredis "bitcask-go/redis"
	"errors"
	"github.com/tidwall/redcon"
	"strings"
)

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	"set": set,
	"get": get,
}

type BitcaskClient struct {
	server *BitcaskServer
	db     *bitcaskredis.DataStructure
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	handler, ok := supportedCommands[command]
	if !ok {
		conn.WriteError("ERR unknown command '" + command + "'")
		return
	}

	client, _ := conn.Context().(*BitcaskClient)

	switch command {
	case "ping":
		conn.WriteString("PONG")
	case "quit":
		_ = conn.Close()
		conn.WriteString("OK")
	default:
		res, err := handler(client, cmd.Args[1:])
		if err != nil {
			if errors.Is(err, bitcask.ErrKeyNotFound) {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}

func newWrongNumOfArgsError(cmd string) error {
	return errors.New("ERR wrong number of arguments for '" + cmd + "' command")
}

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("set")
	}

	key, value := args[0], args[1]
	if err := cli.db.Set(key, 0, value); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("get")
	}

	key := args[0]
	value, err := cli.db.Get(key)
	if err != nil {
		return nil, err
	}
	return value, nil
}
