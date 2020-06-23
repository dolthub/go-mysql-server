package function

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
	"math"
	"testing"
	"time"
)

func TestAsciiFunc(t *testing.T) {
	f := NewUnaryFunc("ascii", sql.Uint8, AsciiFunc)
	tf := NewTestFactory(f.Fn)
	tf.AddSucceeding(nil, nil)
	tf.AddSucceeding(uint8(115), "string")
	tf.AddSucceeding(uint8(49), true)
	tf.AddSucceeding(uint8(50), time.Date(2020, 1,1, 0,0,0,0, time.UTC))
	tf.AddSignedVariations(uint8(48), 0)
	tf.AddUnsignedVariations(uint8(48), 0)
	tf.AddFloatVariations(uint8(54), 6.0)
	tf.Test(t, nil, nil)
}


func TestHexFunc(t *testing.T) {
	f := NewUnaryFunc("hex", sql.Text, HexFunc)
	tf := NewTestFactory(f.Fn)
	tf.AddSucceeding(nil, nil)
	tf.AddSignedVariations("FFFFFFFFFFFFFFFF", -1)
	tf.AddUnsignedVariations("5", 5)
	tf.AddFloatVariations("5", 4.5)
	tf.AddFloatVariations("5", 5.4)
	tf.AddSucceeding("FFFFFFFFFFFFFFFF", uint64(math.MaxUint64))
	tf.AddSucceeding("74657374","test")
	tf.AddSignedVariations("FFFFFFFFFFFFFFF0", -16)
	tf.AddSignedVariations("FFFFFFFFFFFFFF00", -256)
	tf.AddSignedVariations("FFFFFFFFFFFFFE00", -512)
	tf.AddFloatVariations("FFFFFFFFFFFFFFFF", -0.5)
	tf.AddFloatVariations("FFFFFFFFFFFFFFFF", -1.4)
	tf.AddSucceeding("323032302D30322D30342031343A31303A33322E35", time.Date(2020, 2,4, 14,10,32,500000000, time.UTC))
	tf.AddSucceeding("323032302D30322D30342031343A31303A33322E30303035", time.Date(2020, 2,4, 14,10,32,500000, time.UTC))
	tf.AddSucceeding("323032302D30322D30342031343A31303A33322E303030303035", time.Date(2020, 2,4, 14,10,32,5000, time.UTC))
	tf.AddSucceeding("323032302D30322D30342031343A31303A3332", time.Date(2020, 2,4, 14,10,32,500, time.UTC))

	tf.Test(t, nil, nil)
}

func TestUnhexFunc(t *testing.T) {
	f := NewUnaryFunc("unhex", sql.Text, UnhexFunc)
	tf := NewTestFactory(f.Fn)
	tf.AddSucceeding(nil, nil)
	tf.AddSucceeding("MySQL", "4D7953514C")
	tf.AddSucceeding("\x01#Eg\x89\xab\xcd\xef", "0123456789abcdef")
	tf.AddSucceeding(nil, "gh")
	tf.AddSignedVariations("5", 35)
	tf.AddSignedVariations(nil, -1)
	tf.AddUnsignedVariations("5", 35)
	tf.AddFloatVariations(nil, 35.5)
	tf.AddSucceeding(nil, time.Now())

	tf.Test(t, nil, nil)
}

func TestBinFunc(t *testing.T) {
	f := NewUnaryFunc("bin", sql.Text, BinFunc)
	tf := NewTestFactory(f.Fn)
	tf.AddSucceeding(nil, nil)
	tf.AddSucceeding("1100", "12")
	tf.AddSucceeding("0", "TEST")
	tf.AddSucceeding("11111100100", time.Date(2020, 1,1, 0,0,0,0, time.UTC))
	tf.AddSignedVariations("1100", 12)
	tf.AddUnsignedVariations("1100", 12)
	tf.AddFloatVariations("1100", 12.5)
	tf.AddSignedVariations("1111111111111111111111111111111111111111111111111111111111110100", -12)
	tf.AddFloatVariations("1111111111111111111111111111111111111111111111111111111111110100", -12.5)
	tf.Test(t, nil, nil)
}

func TestBitLength(t *testing.T) {
	f := NewUnaryFunc("bin", sql.Int32, BitLengthFunc)
	tf := NewTestFactory(f.Fn)
	tf.AddSucceeding(nil, nil)
	tf.AddSucceeding(32, "test")
	tf.AddSucceeding(8, true)
	tf.AddSucceeding(8, int8(0))
	tf.AddSucceeding(8, uint8(0))
	tf.AddSucceeding(16, int16(0))
	tf.AddSucceeding(16, uint16(0))
	tf.AddSucceeding(32, uint32(0))
	tf.AddSucceeding(32, int32(0))
	tf.AddSucceeding(32, uint(0))
	tf.AddSucceeding(32, 0)
	tf.AddSucceeding(64, uint64(0))
	tf.AddSucceeding(64, int64(0))
	tf.AddSucceeding(64, float64(0))
	tf.AddSucceeding(32, float32(0))
	tf.AddSucceeding(128, time.Now())
	tf.Test(t, nil, nil)
}