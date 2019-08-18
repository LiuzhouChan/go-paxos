// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
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

package logdb

import (
	"os"
	"path/filepath"
	"syscall"
)

func directIOSupported(dir string) bool {
	testfp := filepath.Join(dir, ".direct_io_test_safe_to_delete.gopaxos")
	defer os.Remove(testfp)
	f, err := os.OpenFile(testfp, os.O_CREATE|syscall.O_DIRECT, 0x777)
	if err != nil {
		plog.Errorf("direct io failed: %v", err)
		return false
	}
	f.Close()
	return true
}
