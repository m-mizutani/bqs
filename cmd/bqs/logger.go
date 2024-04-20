package main

import (
	"errors"
	"io"
	"log/slog"

	"github.com/fatih/color"
	"github.com/m-mizutani/clog"
)

var logger *slog.Logger = slog.Default()

func configureLogger(level string, w io.Writer) error {
	levelMap := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}

	logLevel, ok := levelMap[level]
	if !ok {
		return errors.New("invalid log level")
	}

	handler := clog.New(
		clog.WithWriter(w),
		clog.WithLevel(logLevel),
		// clog.WithReplaceAttr(filter),
		clog.WithSource(true),
		// clog.WithTimeFmt("2006-01-02 15:04:05"),
		clog.WithColorMap(&clog.ColorMap{
			Level: map[slog.Level]*color.Color{
				slog.LevelDebug: color.New(color.FgGreen, color.Bold),
				slog.LevelInfo:  color.New(color.FgCyan, color.Bold),
				slog.LevelWarn:  color.New(color.FgYellow, color.Bold),
				slog.LevelError: color.New(color.FgRed, color.Bold),
			},
			LevelDefault: color.New(color.FgBlue, color.Bold),
			Time:         color.New(color.FgWhite),
			Message:      color.New(color.FgHiWhite),
			AttrKey:      color.New(color.FgHiCyan),
			AttrValue:    color.New(color.FgHiWhite),
		}),
	)
	logger = slog.New(handler)

	return nil
}
