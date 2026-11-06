// Command zkcli is a small command line client.
//
//	zkcli -servers 127.0.0.1:2181 create [-e] [-s] /path [data]
//	zkcli -servers 127.0.0.1:2181 get /path
//	zkcli -servers 127.0.0.1:2181 set /path data [version]
//	zkcli -servers 127.0.0.1:2181 ls /path
//	zkcli -servers 127.0.0.1:2181 stat /path
//	zkcli -servers 127.0.0.1:2181 rm /path [version]
//	zkcli -servers 127.0.0.1:2181 watch /path
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"zookeeper/client"
	"zookeeper/zk"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: zkcli [-servers list] command args
commands:
  create [-e] [-s] path [data]   create a znode (-e ephemeral, -s sequential)
  get path                       print data and version
  set path data [version]        write data
  ls path                        list children
  stat path                      print metadata
  rm path [version]              delete a znode
  watch path                     wait for the next change of path`)
	os.Exit(2)
}

func main() {
	servers := flag.String("servers", "127.0.0.1:2181", "comma separated server addresses")
	timeout := flag.Duration("timeout", 10*time.Second, "session timeout")
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		usage()
	}
	c, err := client.Connect(client.Config{Servers: strings.Split(*servers, ","), SessionTimeout: *timeout})
	check(err)
	defer c.Close()

	version := func(i int) int32 {
		if len(args) > i {
			v, err := strconv.Atoi(args[i])
			check(err)
			return int32(v)
		}
		return zk.AnyVersion
	}

	switch cmd := args[0]; cmd {
	case "create":
		var flags zk.CreateFlag
		rest := args[1:]
		for len(rest) > 0 && strings.HasPrefix(rest[0], "-") {
			switch rest[0] {
			case "-e":
				flags |= zk.FlagEphemeral
			case "-s":
				flags |= zk.FlagSequential
			default:
				usage()
			}
			rest = rest[1:]
		}
		if len(rest) == 0 {
			usage()
		}
		var data []byte
		if len(rest) > 1 {
			data = []byte(rest[1])
		}
		p, err := c.Create(rest[0], data, flags)
		check(err)
		fmt.Println(p)
		if flags&zk.FlagEphemeral != 0 {
			fmt.Println("(ephemeral: holding the session until interrupted)")
			select {}
		}
	case "get":
		check(c.Sync(args[1]))
		data, st, err := c.GetData(args[1], false)
		check(err)
		fmt.Printf("%s\nversion=%d\n", data, st.Version)
	case "set":
		if len(args) < 3 {
			usage()
		}
		st, err := c.SetData(args[1], []byte(args[2]), version(3))
		check(err)
		fmt.Printf("version=%d\n", st.Version)
	case "ls":
		check(c.Sync(args[1]))
		kids, err := c.GetChildren(args[1], false)
		check(err)
		for _, k := range kids {
			fmt.Println(k)
		}
	case "stat":
		check(c.Sync(args[1]))
		st, err := c.Exists(args[1], false)
		check(err)
		if st == nil {
			check(zk.ErrNoNode)
		}
		fmt.Printf("%+v\n", *st)
	case "rm":
		check(c.Delete(args[1], version(2)))
	case "watch":
		_, _, ch, err := c.GetDataW(args[1])
		if err == zk.ErrNoNode {
			_, ch, err = c.ExistsW(args[1])
		}
		check(err)
		fmt.Println(<-ch)
	default:
		usage()
	}
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
