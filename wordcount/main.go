package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

var (
	wordRegexp = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
	empty      = beam.NewCounter("extract", "emptyLines")
	lineLen    = beam.NewDistribution("extract", "lineLenDistro")
)

func main() {
	input := flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "input file uri")
	output := flag.String("output", "counts.txt", "output file uri")

	flag.Parse()
	beam.Init()

	if *output == "" {
		log.Fatal("output must not be blank")
	}

	p := beam.NewPipeline()
	rootScope := p.Root()
	lines := textio.Read(rootScope, *input)
	countsScope := rootScope.Scope("Count words")
	separated := beam.ParDo(countsScope, func(ctx context.Context, line string, emit func(string)) {
		lineLen.Update(ctx, int64(len(line)))
		if len(strings.TrimSpace(line)) == 0 {
			empty.Inc(ctx, 1)
		}
		for _, word := range wordRegexp.FindAllString(line, -1) {
			emit(word)
		}
	}, lines)
	counted := stats.Count(rootScope, separated)
	formated := beam.ParDo(rootScope, func(w string, c int) string {
		return fmt.Sprintf("%s: %v", w, c)
	}, counted)
	textio.Write(rootScope, *output, formated)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("failed to execute job: %#v", err)
	}
}
