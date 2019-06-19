// Copyright 2019 Liuzhou Chen (llz.chann@gmail.com).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logger

import (
	"github.com/sirupsen/logrus"
)

// CreateLogrusLog creates an ILogger instance based on logrus
func CreateLogrusLog(pkgName string) ILogger {
	return &logrusLog{
		logger: logrus.New(),
	}
}

type logrusLog struct {
	logger *logrus.Logger
}

func (c *logrusLog) SetLevel(level LogLevel) {
	var cl logrus.Level
	if level == CRITICAL {
		cl = logrus.FatalLevel
	} else if level == ERROR {
		cl = logrus.ErrorLevel
	} else if level == WARNING {
		cl = logrus.WarnLevel
	} else if level == INFO {
		cl = logrus.InfoLevel
	} else if level == DEBUG {
		cl = logrus.DebugLevel
	} else {
		panic("unexpected level")
	}
	c.logger.SetLevel(cl)
}

func (c *logrusLog) Debugf(format string, args ...interface{}) {
	c.logger.Debugf(format, args...)
}

func (c *logrusLog) Infof(format string, args ...interface{}) {
	c.logger.Infof(format, args...)
}

func (c *logrusLog) Warningf(format string, args ...interface{}) {
	c.logger.Warningf(format, args...)
}

func (c *logrusLog) Errorf(format string, args ...interface{}) {
	c.logger.Errorf(format, args...)
}

func (c *logrusLog) Panicf(format string, args ...interface{}) {
	c.logger.Panicf(format, args...)
}
