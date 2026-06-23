// Command quorumctl is a CLI client for a quorum cluster.
//
// Usage:
//
//	quorumctl --addr :7000 put <key> <value>
//	quorumctl --addr :7000 get <key>
//	quorumctl --addr :7000 delete <key>
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/adityasingh/quorum/pkg/client"
)

func main() {
	addr := flag.String("addr", ":7000", "node address (host:port)")
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(2)
	}

	c, err := client.Dial(*addr)
	if err != nil {
		fatalf("%v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch cmd := args[0]; cmd {
	case "put":
		if len(args) != 3 {
			fatalf("usage: quorumctl put <key> <value>")
		}
		if err := c.Put(ctx, args[1], args[2]); err != nil {
			fatalf("put: %v", err)
		}
		fmt.Println("OK")

	case "get":
		if len(args) != 2 {
			fatalf("usage: quorumctl get <key>")
		}
		value, found, err := c.Get(ctx, args[1])
		if err != nil {
			fatalf("get: %v", err)
		}
		if !found {
			fmt.Fprintln(os.Stderr, "(not found)")
			os.Exit(1)
		}
		fmt.Println(value)

	case "delete":
		if len(args) != 2 {
			fatalf("usage: quorumctl delete <key>")
		}
		existed, err := c.Delete(ctx, args[1])
		if err != nil {
			fatalf("delete: %v", err)
		}
		if existed {
			fmt.Println("OK")
		} else {
			fmt.Println("(not found)")
		}

	default:
		fatalf("unknown command %q", cmd)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: quorumctl [--addr host:port] <put|get|delete> ...")
	flag.PrintDefaults()
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
