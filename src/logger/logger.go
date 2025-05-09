package logger

import (
	"fmt"
	"log"
	"os"

	"github.com/davecgh/go-spew/spew"
)

const (
	DEBUG = iota
	INFO
	WARN
	ERROR
)

var logLevel = ERROR

var (
	debugLogger *log.Logger
	infoLogger  *log.Logger
	warnLogger  *log.Logger
	errorLogger *log.Logger
	logFile     *os.File
)

func InitLogger(output *os.File) {
	debugLogger = log.New(output, "DEBUG: ", log.LstdFlags|log.Lshortfile)
	infoLogger = log.New(output, "INFO: ", log.LstdFlags)
	warnLogger = log.New(output, "WARN: ", log.LstdFlags)
	errorLogger = log.New(output, "ERROR: ", log.LstdFlags)
}

func Log(level int, format string, v ...interface{}) {
	message := fmt.Sprintf(format, v...)
	if level >= logLevel {
		switch level {
		case DEBUG:
			debugLogger.Println(message)
		case INFO:
			infoLogger.Println(message)
		case WARN:
			warnLogger.Println(message)
		case ERROR:
			errorLogger.Println(message)
		}
	}
}

func Dump(level int, label string, v interface{}) {
	if level >= logLevel {
		Log(level, "Dumping %s", label)
		spew.Fdump(logFile, v)
	}
}
