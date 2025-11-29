package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
)

var bufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

type PrettyHandlerOptions struct {
	SlogOpts          slog.HandlerOptions
	UseColor          bool
	ShowSource        bool
	FullSource        bool
	CompactJSON       bool
	TimeFormat        string
	LevelWidth        int
	DisableTimestamp  bool
	FieldSeparator    string
	MaxFieldLength    int
	SortKeys          bool
	DisableHTMLEscape bool
}

func DefaultOptions() PrettyHandlerOptions {
	return PrettyHandlerOptions{
		SlogOpts: slog.HandlerOptions{
			Level: slog.LevelInfo,
		},
		UseColor:          true,
		ShowSource:        true,
		FullSource:        false,
		CompactJSON:       false,
		TimeFormat:        time.RFC3339,
		LevelWidth:        7,
		DisableTimestamp:  false,
		FieldSeparator:    " | ",
		MaxFieldLength:    0,
		SortKeys:          false,
		DisableHTMLEscape: true,
	}
}

type PrettyHandler struct {
	opts   PrettyHandlerOptions
	writer io.Writer
	mu     *sync.Mutex
	groups []string
	attrs  []slog.Attr

	colorTime    func(...any) string
	colorLevel   map[slog.Level]func(...any) string
	colorMessage func(...any) string
	colorSource  func(...any) string
	colorFields  func(...any) string
	colorError   func(...any) string
}

func NewPrettyHandler(w io.Writer, opts *PrettyHandlerOptions) *PrettyHandler {
	if opts == nil {
		defaultOpts := DefaultOptions()
		opts = &defaultOpts
	}

	if opts.TimeFormat == "" {
		opts.TimeFormat = time.RFC3339
	}
	if opts.LevelWidth < 5 {
		opts.LevelWidth = 7
	}
	if opts.FieldSeparator == "" {
		opts.FieldSeparator = " | "
	}

	h := &PrettyHandler{
		opts:   *opts,
		writer: w,
		mu:     &sync.Mutex{},
		groups: make([]string, 0),
		attrs:  make([]slog.Attr, 0),
	}
	h.initColorFuncs()

	return h
}

func (h *PrettyHandler) initColorFuncs() {
	if !h.opts.UseColor {
		noColor := func(a ...any) string { return fmt.Sprint(a...) }
		h.colorTime = noColor
		h.colorMessage = noColor
		h.colorSource = noColor
		h.colorFields = noColor
		h.colorError = noColor
		h.colorLevel = make(map[slog.Level]func(...any) string)
		for _, level := range []slog.Level{
			slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError,
		} {
			h.colorLevel[level] = noColor
		}
		return
	}

	h.colorTime = color.New(color.FgHiBlack).SprintFunc()
	h.colorMessage = color.New(color.FgCyan).SprintFunc()
	h.colorSource = color.New(color.FgHiBlack).SprintFunc()
	h.colorFields = color.New(color.FgWhite).SprintFunc()
	h.colorError = color.New(color.FgRed, color.Bold).SprintFunc()

	h.colorLevel = map[slog.Level]func(...any) string{
		slog.LevelDebug: color.New(color.FgMagenta).SprintFunc(),
		slog.LevelInfo:  color.New(color.FgBlue).SprintFunc(),
		slog.LevelWarn:  color.New(color.FgYellow).SprintFunc(),
		slog.LevelError: color.New(color.FgRed).SprintFunc(),
	}
}

func (h *PrettyHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.opts.SlogOpts.Level.Level()
}

func (h *PrettyHandler) Handle(ctx context.Context, r slog.Record) error {
	buf := bufPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		bufPool.Put(buf)
	}()

	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.opts.DisableTimestamp {
		timestamp := r.Time.Format(h.opts.TimeFormat)
		buf.WriteString(h.colorTime(timestamp))
		buf.WriteString(h.opts.FieldSeparator)
	}

	level := h.formatLevel(r.Level)
	buf.WriteString(level)
	buf.WriteString(h.opts.FieldSeparator)

	if h.opts.ShowSource {
		source := h.extractSource(r.PC)
		if source != "" {
			buf.WriteString(h.colorSource(source))
			buf.WriteString(h.opts.FieldSeparator)
		}
	}

	buf.WriteString(h.colorMessage(r.Message))

	attrs := h.collectAttributes(r)
	if len(attrs) > 0 {
		buf.WriteString(h.opts.FieldSeparator)
		if err := h.formatAttributes(buf, attrs); err != nil {
			fmt.Fprintf(buf, fmt.Sprintf("(error formatting attributes: %v)", err))
		}
	}

	buf.WriteByte('\n')
	_, err := h.writer.Write(buf.Bytes())
	return err
}

func (h *PrettyHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	newHandler := &PrettyHandler{
		opts:   h.opts,
		writer: h.writer,
		mu:     &sync.Mutex{},
		groups: append([]string(nil), h.groups...),
		attrs:  append(append([]slog.Attr(nil), h.attrs...), attrs...),
	}
	newHandler.initColorFuncs()

	return newHandler
}

func (h *PrettyHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	newHandler := &PrettyHandler{
		opts:   h.opts,
		writer: h.writer,
		mu:     &sync.Mutex{},
		groups: append(append([]string(nil), h.groups...), name),
		attrs:  append([]slog.Attr(nil), h.attrs...),
	}
	newHandler.initColorFuncs()

	return newHandler
}

func (h *PrettyHandler) formatLevel(level slog.Level) string {
	levelStr := strings.ToUpper(level.String())

	if h.opts.LevelWidth > 0 {
		levelStr = fmt.Sprintf("%-*s", h.opts.LevelWidth, levelStr)
	}

	if colorFunc, ok := h.colorLevel[level]; ok {
		return colorFunc(levelStr)
	}

	if level > slog.LevelError {
		return h.colorError(levelStr)
	}
	return levelStr
}

func (h *PrettyHandler) extractSource(pc uintptr) string {
	if pc == 0 {
		return ""
	}

	frames := runtime.CallersFrames([]uintptr{pc})
	frame, _ := frames.Next()

	if frame.Function == "" {
		return ""
	}

	file := frame.File
	if !h.opts.FullSource {
		file = filepath.Base(file)
	}

	source := fmt.Sprintf("%s:%d", file, frame.Line)

	if h.opts.SlogOpts.AddSource {
		funcName := frame.Function
		if idx := strings.LastIndex(funcName, "."); idx >= 0 {
			funcName = funcName[idx+1:]
		}
		source = fmt.Sprintf("%s:%s", source, funcName)
	}

	return source
}

func (h *PrettyHandler) collectAttributes(r slog.Record) map[string]any {
	attrs := make(map[string]any)

	current := attrs
	for _, group := range h.groups {
		nested := make(map[string]any)
		current[group] = nested
		current = nested
	}

	for _, attr := range h.attrs {
		h.addAttribute(current, attr)
	}

	r.Attrs(func(attr slog.Attr) bool {
		h.addAttribute(current, attr)
		return true
	})

	h.cleanEmptyGroups(attrs)

	return attrs
}

func (h *PrettyHandler) addAttribute(attrs map[string]any, attr slog.Attr) {
	value := attr.Value.Resolve()

	if value.Kind() == slog.KindGroup {
		group := make(map[string]any)
		for _, groupAttr := range value.Group() {
			h.addAttribute(group, groupAttr)
		}
		if len(group) > 0 {
			attrs[attr.Key] = group
		}
		return
	}

	var v any
	switch value.Kind() {
	case slog.KindTime:
		v = value.Time().Format(h.opts.TimeFormat)
	case slog.KindDuration:
		v = value.Duration().String()
	case slog.KindAny:
		v = value.Any()
		if h.opts.MaxFieldLength > 0 {
			if str, ok := v.(string); ok && len(str) > h.opts.MaxFieldLength {
				v = str[:h.opts.MaxFieldLength] + "..."
			}
		}
	default:
		v = value.Any()
	}

	attrs[attr.Key] = v
}

func (h *PrettyHandler) cleanEmptyGroups(attrs map[string]any) {
	for key, value := range attrs {
		if nested, ok := value.(map[string]any); ok {
			h.cleanEmptyGroups(nested)
			if len(nested) == 0 {
				delete(attrs, key)
			}
		}
	}
}

func (h *PrettyHandler) formatAttributes(buf *bytes.Buffer, attrs map[string]any) error {
	if len(attrs) == 0 {
		return nil
	}

	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(!h.opts.DisableHTMLEscape)

	if h.opts.CompactJSON {
		encoder.SetIndent("", "")
	} else {
		encoder.SetIndent("", "  ")
	}

	var jsonBuf bytes.Buffer
	encoder = json.NewEncoder(&jsonBuf)
	encoder.SetEscapeHTML(!h.opts.DisableHTMLEscape)
	if h.opts.CompactJSON {
		encoder.SetIndent("", "")
	} else {
		encoder.SetIndent("", "  ")
	}

	if err := encoder.Encode(attrs); err != nil {
		return err
	}

	result := bytes.TrimRight(jsonBuf.Bytes(), "\n")

	buf.WriteString(h.colorFields(string(result)))

	return nil
}
