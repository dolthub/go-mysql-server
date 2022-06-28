// Copyright 2022 Dolthub, Inc.
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

package sql

import (
	"bytes"
	"reflect"
	"strings"
	"unsafe"
)

// CollatedString is a string whose data aligns with the associated collation.
//TODO: mention character set coercion when a parameter has a different character set than the caller
//TODO: mention collation coercion when a parameter has the same character set but different collation
type CollatedString struct {
	collation CollationID
	data      []byte
}

// CollatedStringMapKey is a map key for a CollatedString. As a CollatedString cannot be used directly as a key for
// maps, this works as a stand-in.
type CollatedStringMapKey string

// CollatedStringSlice is a slice of CollatedString values.
type CollatedStringSlice []CollatedString

//TODO: doc (warn to not modify)
func LoadCollatedString(collation CollationID, data []byte) CollatedString {
	return CollatedString{
		collation: collation,
		data:      data,
	}
}

//TODO: doc (all Go strings implicitly use utf8mb4_0900_bin collation)
func ToCollatedString(s string) CollatedString {
	// Most efficient way to get the byte slice of a string according to https://stackoverflow.com/a/69231355
	const MaxInt32 = 1<<31 - 1
	return CollatedString{
		collation: Collation_utf8mb4_0900_bin,
		data:      (*[MaxInt32]byte)(unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&s)).Data))[: len(s)&MaxInt32 : len(s)&MaxInt32],
	}
}

//TODO: doc
func (cs CollatedString) ByteLength() int {
	return len(cs.data)
}

//TODO: doc
func (cs CollatedString) ChangeCollation(newCollation CollationID) (CollatedString, error) {
	//TODO: check that the string is all ASCII? https://dev.mysql.com/doc/refman/8.0/en/charset-repertoire.html
	//I don't think all ASCII needs to rewrite the string, but I need to do further research on this.
	//For now I'm just changing the collation without error, but THIS IS WRONG.
	return CollatedString{
		collation: newCollation,
		data:      cs.data,
	}, nil
}

//TODO: doc
func (cs CollatedString) Collation() CollationID {
	return cs.collation
}

//TODO: doc
func (cs CollatedString) Compare(other CollatedString) int {
	//TODO: different collations
	return strings.Compare(cs.GoString(), other.GoString())
}

//TODO: doc
func (cs CollatedString) Contains(str CollatedString) bool {
	//TODO: hack for now
	return strings.Contains(cs.GoString(), str.GoString())
}

//TODO: doc (warn to not modify)
func (cs CollatedString) Data() []byte {
	return cs.data
}

//TODO: doc
func (cs CollatedString) Equals(other CollatedString) bool {
	//TODO: should I consider character sets or collations? two different character sets will not encode their data the same
	return bytes.Equal(cs.data, other.data)
}

//TODO: doc
func (cs CollatedString) MapKey() CollatedStringMapKey {
	// We can't use byte slices as keys in Go, so we pretend that it's a string instead
	return CollatedStringMapKey(*(*string)(unsafe.Pointer(&cs.data)))
}

//TODO: doc (warn that string should be ASCII due to default collation)
func (cs CollatedString) GoSplit(str string) []CollatedString {
	return cs.Split(ToCollatedString(str).ReinterpretData(cs.collation))
}

//TODO: doc
func (cs CollatedString) GoString() string {
	//TODO: with this, different character sets may produce garbage strings so this will need to rewrite the data
	return *(*string)(unsafe.Pointer(&cs.data))
}

//TODO: doc (warn that string should be ASCII due to default collation)
func (cs CollatedString) GoTrimLeft(cutset string) CollatedString {
	//TODO: hack for now
	return ToCollatedString(strings.TrimLeft(cs.GoString(), cutset)).ReinterpretData(cs.collation)
}

//TODO: doc (warn that string should be ASCII due to default collation)
func (cs CollatedString) GoTrimRight(cutset string) CollatedString {
	//TODO: hack for now
	return ToCollatedString(strings.TrimRight(cs.GoString(), cutset)).ReinterpretData(cs.collation)
}

//TODO: doc
func (cs CollatedString) IsEmpty() bool {
	return len(cs.data) == 0
}

//TODO: doc
func (cs CollatedString) Length() int {
	//TODO: this is all hack since we only support 'binary' and 'utf8mb4' character sets right now
	if cs.collation == Collation_binary {
		return len(cs.data)
	}
	//This converts the collated string into a Go string, and converts that to runes, and then counts the runes
	return len([]rune(cs.GoString()))
}

//TODO: doc (warn that new string may be garbage as we don't check for validity)
func (cs CollatedString) PadLeft(char byte, amount int) CollatedString {
	dataLen := len(cs.data)
	newData := make([]byte, dataLen+amount)
	copy(newData[dataLen:], cs.data)
	for i := 0; i < amount; i++ {
		newData[i] = char
	}
	return CollatedString{
		collation: cs.collation,
		data:      newData,
	}

}

//TODO: doc (warn that new string may be garbage as we don't check for validity)
func (cs CollatedString) PadRight(char byte, amount int) CollatedString {
	dataLen := len(cs.data)
	newData := make([]byte, dataLen+amount)
	copy(newData, cs.data)
	for i := 0; i < amount; i++ {
		newData[i+dataLen] = char
	}
	return CollatedString{
		collation: cs.collation,
		data:      newData,
	}
}

//TODO: doc
func (cs CollatedString) ReinterpretData(newCollation CollationID) CollatedString {
	return CollatedString{
		collation: newCollation,
		data:      cs.data,
	}
}

//TODO: doc
func (cs CollatedString) Split(str CollatedString) []CollatedString {
	//TODO: hack for now, need to replace with actual split logic for each character set
	goCs := cs.GoString()
	goStr := str.GoString()
	goSplitStrs := strings.Split(goCs, goStr)
	ret := make([]CollatedString, len(goSplitStrs))
	for i, goSplitStr := range goSplitStrs {
		ret[i] = ToCollatedString(goSplitStr).ReinterpretData(cs.collation)
	}
	return ret
}

//TODO: doc
func (cs CollatedString) ToLower() CollatedString {
	//TODO: hack for now
	return ToCollatedString(strings.ToLower(cs.GoString())).ReinterpretData(cs.collation)
}

//TODO: doc
func (cs CollatedString) ToUpper() CollatedString {
	//TODO: hack for now
	return ToCollatedString(strings.ToUpper(cs.GoString())).ReinterpretData(cs.collation)
}

//TODO: doc
func (cs CollatedString) TrimLeft(cutset CollatedString) CollatedString {
	//TODO: hack for now
	return ToCollatedString(strings.TrimLeft(cs.GoString(), cutset.GoString())).ReinterpretData(cs.collation)
}

//TODO: doc
func (cs CollatedString) TrimRight(cutset CollatedString) CollatedString {
	//TODO: hack for now
	return ToCollatedString(strings.TrimRight(cs.GoString(), cutset.GoString())).ReinterpretData(cs.collation)
}

//TODO: doc (warn that separator should be ASCII due to default collation, collation of first element is used if non-empty, separator character set is ignored)
func (css CollatedStringSlice) GoJoin(separator string) CollatedString {
	return css.Join(ToCollatedString(separator))
}

//TODO: doc (collation of first element is used if non-empty & separator character set is ignored)
func (css CollatedStringSlice) Join(separator CollatedString) CollatedString {
	separatorLen := len(separator.data)
	cssLen := len(css)
	if cssLen == 0 {
		return CollatedString{
			collation: Collation_Default,
			data:      nil,
		}
	}
	// First pass gets the size required for the final string
	totalSize := len(separator.data) * (cssLen - 1)
	for i := 0; i < cssLen; i++ {
		totalSize += len(css[i].data)
	}
	outputData := make([]byte, totalSize)
	// Second pass writes to the final string
	copy(outputData, css[0].data)
	currentOutputIndex := len(css[0].data)
	for i := 1; i < cssLen; i++ {
		copy(outputData[currentOutputIndex:], separator.data)
		currentOutputIndex += separatorLen
		copy(outputData[currentOutputIndex:], css[i].data)
		currentOutputIndex += len(css[i].data)
	}
	return CollatedString{
		collation: css[0].collation,
		data:      outputData,
	}
}
