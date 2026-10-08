// Package sentences preserves the SDK's incremental BlingFire sentence rules.
package sentences

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

//go:embed blingfire.wasm
var wasm []byte

// Cache compiled code only. Each operation has separate WASM memory and functions.
var compilationCache = wazero.NewCompilationCache()

type Detector struct {
	runtime wazero.Runtime
	module  api.Module
}

func New(ctx context.Context) (*Detector, error) {
	r := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().WithCompilationCache(compilationCache).WithMemoryLimitPages(1024).WithCloseOnContextDone(true))
	// The host does not cache memory views across calls, so growth needs no update.
	if _, err := r.NewHostModuleBuilder("env").NewFunctionBuilder().WithFunc(func(uint32) {}).Export("emscripten_notify_memory_growth").Instantiate(ctx); err != nil {
		r.Close(ctx)
		return nil, err
	}
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, r); err != nil {
		r.Close(ctx)
		return nil, err
	}
	m, err := r.InstantiateWithConfig(ctx, wasm, wazero.NewModuleConfig().WithStartFunctions("_initialize"))
	if err != nil {
		r.Close(ctx)
		return nil, err
	}
	return &Detector{r, m}, nil
}

func (d *Detector) Close() { _ = d.runtime.Close(context.Background()) }

func (d *Detector) ends(ctx context.Context, text string) ([]int, error) {
	if strings.TrimSpace(text) == "" {
		return nil, nil
	}
	capacity := len(text)*3 + 16
	allocate := d.module.ExportedFunction("malloc")
	source, err := allocate.Call(ctx, uint64(len(text)+1))
	if err != nil {
		return nil, err
	}
	defer d.module.ExportedFunction("free").Call(ctx, source[0])
	output, err := allocate.Call(ctx, uint64(capacity))
	if err != nil {
		return nil, err
	}
	defer d.module.ExportedFunction("free").Call(ctx, output[0])
	if source[0] == 0 || output[0] == 0 || !d.module.Memory().Write(uint32(source[0]), append([]byte(text), 0)) {
		return nil, fmt.Errorf("sentence detector allocation failed")
	}
	result, err := d.module.ExportedFunction("TextToSentences").Call(ctx, source[0], uint64(len(text)), output[0], uint64(capacity))
	if err != nil {
		return nil, err
	}
	length := int(int32(result[0]))
	if length < 0 || length > capacity {
		return nil, fmt.Errorf("sentence detection failed")
	}
	data, ok := d.module.Memory().Read(uint32(output[0]), uint32(length))
	if !ok {
		return nil, fmt.Errorf("sentence detector returned invalid memory")
	}
	position := 0
	var ends []int
	for _, sentence := range strings.Split(strings.TrimRight(string(data), "\x00"), "\n") {
		if strings.TrimSpace(sentence) == "" {
			continue
		}
		for _, char := range sentence {
			if unicode.IsSpace(char) {
				continue
			}
			for position < len(text) {
				original, size := utf8.DecodeRuneInString(text[position:])
				if original == char || !(unicode.IsSpace(original) || strings.ContainsRune("\u200b\u200e\u200f\ufeff", original)) {
					break
				}
				position += size
			}
			original, size := utf8.DecodeRuneInString(text[position:])
			if position == len(text) || original != char {
				return nil, fmt.Errorf("sentence detector could not preserve source offsets")
			}
			position += size
		}
		ends = append(ends, position)
	}
	return ends, nil
}

// LimitError identifies a sentence exceeding the UTF-8 byte limit.
type LimitError struct{}

func (LimitError) Error() string { return "sentence exceeds the supported byte limit" }

type Buffer struct {
	detector                               *Detector
	pending                                string
	parts                                  strings.Builder
	committed, untilScan, sincePunctuation int
}

func NewBuffer(detector *Detector) *Buffer {
	return &Buffer{detector: detector, untilScan: 1024, sincePunctuation: 16}
}

const triggers = "\r\n.!?\u01c3\u06d4\u061f\u2024\u2026\u203c\u203d\u2048\u2049\u2404\ufe52\uff0e\uff61\u3002\uff1f\uff01\u2028\u2029\u00bf\u00a1"

// Feed emits original text through a blocking callback, which supplies transport backpressure.
func (b *Buffer) Feed(ctx context.Context, fragment string, final bool, emit func(string) error) error {
	for _, char := range fragment {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		b.parts.WriteRune(char)
		b.untilScan--
		b.sincePunctuation = min(b.sincePunctuation+1, 16)
		if strings.ContainsRune(triggers, char) {
			b.sincePunctuation = 0
			b.untilScan = min(b.untilScan, 16)
		}
		if b.untilScan == 0 {
			if err := b.scan(ctx, false, emit); err != nil {
				return err
			}
		}
	}
	if final {
		return b.scan(ctx, true, emit)
	}
	return nil
}

func (b *Buffer) scan(ctx context.Context, final bool, emit func(string) error) error {
	b.pending += b.parts.String()
	b.parts.Reset()
	if b.pending == "" {
		return nil
	}
	ends, err := b.detector.ends(ctx, b.pending)
	if err != nil {
		return err
	}
	if final && len(ends) > 0 {
		ends[len(ends)-1] = len(b.pending)
	}
	start := b.committed
	for _, end := range append(append([]int(nil), ends...), len(b.pending)) {
		if end <= start {
			continue
		}
		if end-start > 65536 {
			return LimitError{}
		}
		start = end
	}
	for i, end := range ends {
		if !final && (i == len(ends)-1 || utf8.RuneCountInString(strings.TrimSpace(b.pending[end:])) < 16) {
			continue
		}
		if end <= b.committed {
			continue
		}
		sentence := b.pending[b.committed:end]
		b.committed = end
		if strings.TrimSpace(sentence) != "" {
			if err := emit(sentence); err != nil {
				return err
			}
		}
	}
	if final {
		residual := b.pending[b.committed:]
		if strings.TrimSpace(residual) != "" {
			if err := emit(residual); err != nil {
				return err
			}
		}
		b.pending = ""
		b.committed = 0
		b.untilScan = 1024
		b.sincePunctuation = 16
		return nil
	}
	b.untilScan = 1024
	if b.sincePunctuation < 16 {
		b.untilScan = 16
	}
	for _, end := range ends {
		if b.committed < end && end < len(b.pending) {
			b.untilScan = min(b.untilScan, max(1, 16-utf8.RuneCountInString(strings.TrimLeftFunc(b.pending[end:], unicode.IsSpace))))
			break
		}
	}
	contextStart := b.committed
	for i := 0; i < 128 && contextStart > 0; i++ {
		_, size := utf8.DecodeLastRuneInString(b.pending[:contextStart])
		contextStart -= size
	}
	b.pending = strings.Clone(b.pending[contextStart:])
	b.committed -= contextStart
	return nil
}
