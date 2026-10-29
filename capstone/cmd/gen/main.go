// Command gen writes a deterministic dirty clickstream dataset: go run ./capstone/cmd/gen -out data [-seed 1] [-files 4] [-events 5000]
package main

import (
	"flag"
	"fmt"
	"os"

	"dataflow/capstone/gen"
)

func main() {
	p := gen.DefaultParams()
	out := flag.String("out", "data", "output directory")
	flag.Int64Var(&p.Seed, "seed", p.Seed, "random seed (same seed = same bytes)")
	flag.IntVar(&p.Files, "files", p.Files, "number of files")
	flag.IntVar(&p.EventsPerFile, "events", p.EventsPerFile, "lines per file")
	flag.Parse()
	if err := gen.Generate(*out, p); err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(1)
	}
	fmt.Printf("wrote %d files of %d lines to %s\n", p.Files, p.EventsPerFile, *out)
}
