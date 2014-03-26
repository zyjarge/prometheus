package opentsdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

var (
	tagValueTests = []struct {
		tv   TagValue
		json []byte
	}{
		{TagValue("foo-bar-42"), []byte(`"foo-bar-42"`)},
		{TagValue("foo_bar_42"), []byte(`"foo__bar__42"`)},
		{TagValue("http://example.org:8080"), []byte(`"http_.//example.org_.8080"`)},
		{TagValue("Björn's email: bjoern@soundcloud.com"), []byte(`"Bj_C3_B6rn_27s_20email_._20bjoern_40soundcloud.com"`)},
		{TagValue("日"), []byte(`"_E6_97_A5"`)},
	}
)

func TestTagValueMarshaling(t *testing.T) {
	for i, tt := range tagValueTests {
		json, err := json.Marshal(tt.tv)
		if err != nil {
			t.Errorf("%d. Marshal(%q) returned err: %s", i, tt.tv, err)
		} else {
			if !bytes.Equal(json, tt.json) {
				t.Errorf(
					"%d. Marshal(%q) => %s, want %s",
					i, tt.tv, json, tt.json,
				)
			}
		}
	}
}

func TestTagValueUnMarshaling(t *testing.T) {
	for i, tt := range tagValueTests {
		var tv TagValue
		err := json.Unmarshal(tt.json, &tv)
		if err != nil {
			t.Errorf("%d. Unmarshal(%s, &str) returned err: %s", i, tt.json, err)
		} else {
			if tv != tt.tv {
				t.Errorf(
					"%d. Unmarshal(%s, &str) => str==%q, want %q",
					i, tt.json, tv, tt.tv,
				)
			}
		}
	}
}

func TestTagValueToStringMapMarshaling(t *testing.T) {

	// Create a single TagValueToStringMap and its expected JSON
	// representation.
	m := TagValueToStringMap{}
	var mBuf bytes.Buffer
	mBuf.WriteRune('{')
	for i, tt := range tagValueTests {
		m[tt.tv] = fmt.Sprint(i)
		if i > 0 {
			mBuf.WriteRune(',')
		}
		mBuf.Write(tt.json)
		mBuf.WriteString(fmt.Sprintf(`:"%v"`, i))
	}
	mBuf.WriteByte('}')
	mJSON := mBuf.Bytes()

	// Create a list of two TagValueToStringMaps and its expected JSON
	// representation.
	mm := []TagValueToStringMap{m, m}
	var mmBuf bytes.Buffer
	mmBuf.WriteRune('[')
	mmBuf.Write(mJSON)
	mmBuf.WriteRune(',')
	mmBuf.Write(mJSON)
	mmBuf.WriteRune(']')
	mmJSON := mmBuf.Bytes()

	// Marshaling.
	j, err := json.Marshal(m)
	if err != nil {
		t.Errorf("Marshal(%#v) returned err: %s", m, err)
	} else {
		if !bytes.Equal(j, mJSON) {
			t.Errorf(
				"Marshal(%#v) => %s, want %s",
				m, j, mJSON,
			)
		}
	}
	j, err = json.Marshal(mm)
	if err != nil {
		t.Errorf("Marshal(%#v) returned err: %s", mm, err)
	} else {
		if !bytes.Equal(j, mmJSON) {
			t.Errorf(
				"Marshal(%#v) => %s, want %s",
				mm, j, mmJSON,
			)
		}
	}

	// Unmarshaling.
	var mUnmarshaled TagValueToStringMap
	var mmUnmarshaled []TagValueToStringMap
	err = json.Unmarshal(mJSON, &mUnmarshaled)
	if err != nil {
		t.Errorf(
			"Unmarshal(%s, &m) returned error: %s", mJSON, err)
	}
	if !reflect.DeepEqual(mUnmarshaled, m) {
		t.Errorf(
			"Unmarshal(%s, &m) => m==%#v, want %#v",
			mJSON, mUnmarshaled, m,
		)
	}
	err = json.Unmarshal(mmJSON, &mmUnmarshaled)
	if err != nil {
		t.Errorf(
			"Unmarshal(%s, &mm) returned error: %s", mmJSON, err)
	}
	if !reflect.DeepEqual(mmUnmarshaled, mm) {
		t.Errorf(
			"Unmarshal(%s, &mm) => mm==%#v, want %#v",
			mmJSON, mmUnmarshaled, mm,
		)
	}
}
