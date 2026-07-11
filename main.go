package main

import (
	"fmt"
	"os"

	"github.com/mydearniko/idoud/internal/cli"
)

var version = "dev"

func main() {
	if len(os.Args) == 2 && (os.Args[1] == "--version" || os.Args[1] == "-V") {
		fmt.Printf("idoud %s\n", version)
		return
	}
	os.Exit(cli.Run(os.Args[1:]))
}
