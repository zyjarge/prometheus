// Copyright 2013 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metric

import (
	"code.google.com/p/goprotobuf/proto"
	"github.com/prometheus/prometheus/model"
	dto "github.com/prometheus/prometheus/model/generated"
	"github.com/prometheus/prometheus/storage/raw/leveldb"
)

type curator struct {
	stop       chan bool
	samples    *leveldb.LevelDBPersistence
	watermarks *leveldb.LevelDBPersistence
}

func newCurator(samples, watermarks *LevelDBPersistence) (curator *curator, err error) {
	return curator{
		stop:       make(chan bool),
		samples:    samples,
		watermarks: watermarks,
	}
}

type watermarkDecoder struct{}

func (w watermarkDecoder) DecodeKey(in interface{}) (out interface{}, err error) {
	var (
		key   = &dto.Fingerprint{}
		bytes = in.([]byte)
	)

	err = proto.Unmarshal(bytes, key)
	if err != nil {
		return
	}

	out = model.NewFingerprintFromRowKey(*key.Signature)

	return
}

func (w watermarkDecoder) DecodeValue(in interface{}) (out interface{}, err error) {
	var (
		dto   = &dto.MetricHighWatermark{}
		bytes = in.([]byte)
	)

	err = proto.Unmarshal(bytes, dto)
	if err != nil {
		return
	}

	out = model.NewCurationRemarkFromDTO(dto)

	return
}

type watermarkDiscriminator struct {
	recencyCutOff time.Time
}

func (w watermarkDiscriminator) Filter(_, value interface{}) (result FilterResult) {
	var (
		remark    = value.(model.CurationRemark)
		foundTime = time.Unix(*decodedValue.Timestamp, 0)
	)

	if foundTime.Before(w.recencyCutOff) {
		return storage.SKIP
	}

	return storage.ACCEPT
}

type watermarkOperator struct {
	olderThan     time.Time
	groupSize     int
	curationState *leveldb.LevelDBPersistence
}

func (w watermarkOperator) Operate(key, value interface{}) (err *OperatorError) {
	var (
		fingerprint = key.(model.Fingerprint)
	)

}

func (w watermarkOperator) hasBeenCurated(f model.Fingerprint) (curated bool, err error) {
	curationKey := &dto.CurationKey{
		Fingerprint: f.ToDTO(),
		OlderThan:   proto.Int64(w.olderThan),
		GroupSize:   proto.Int(w.groupSize),
	}

	curated, err = w.curationState.Has(encoding.NewProtocolBufferEncoder(curationKey))

	return
}

func (c curator) run() (err error) {
}
