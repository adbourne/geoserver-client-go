package geoserver

const (
	// levelKey is the key used by the logger to denote the logging level e.g info, debug etc
	levelKey = "level"

	// messageKey is the key used by the logger to denote the log message
	messageKey = "message"

	// urlKey is the key used by the logger to denote the URL used
	urlKey = "url"

	// requestKey is the key used by the logger to denote the request sent
	requestKey = "request"

	// errorKey is the key used by the logger to denote the error
	errorKey = "error"

	// statusKey is the key used by the logger to denote the status
	statusKey = "status"

	// LogLevelDebug is the value for the DEBUG log level
	levelDebug = "debug"

	// levelWarn is the value for the WARN log level
	levelWarn = "warn"
)

// LoggerFunc is an abstraction over the logger.
// It allows for different loggers to be used.
//
// The client log using key value pairs e.g:
//     Log(
//         "message", "Some Message",
//         "level", "info",
//         "foo", "bar",
//     )
//
// If your chosen logger does not accept key value pairs, then the implementation of this function should
// should convert the key value pairs to the appropriate string.
type LoggerFunc interface {
	Log(string, ...interface{})
}
