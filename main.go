package main

import (
	"os"

	"github.com/mydearniko/idoud/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:]))
}
