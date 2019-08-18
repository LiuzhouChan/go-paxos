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

// +build dragonboat_monkeytest

package logdb

const (
	inMonkeyTesting = true
)

// SetEntryBatchSize sets the entry batch size.
func SetEntryBatchSize(sz uint64) {
	batchSize = sz
}

// Disable range delete.
func DisableRangeDelete() {
	useRangeDelete = false
}

// Enable range delete.
func EnableRangeDelete() {
	useRangeDelete = true
}

// SetLogDBInstanceCount set the number of rocksdb instances to use.
func SetLogDBInstanceCount(count uint64) {
	numOfRocksDBInstance = count
}

// SetRDBContextSize set the RDB context related sizes
func SetRDBContextSize(valueSize uint64) {
	RDBContextValueSize = valueSize
}
