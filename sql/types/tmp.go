package types

//
//func convertToUint8(t NumberTypeImpl_, v interface{}) (uint8, bool, error) {
//	switch v := v.(type) {
//	case int:
//		if v < 0 {
//			return uint8(math.MaxUint32 - uint(-v)), true, nil
//		}
//		return uint8(v), false, nil
//	case int16:
//		if v < 0 {
//			return uint8(math.MaxUint32 - uint(-v)), true, nil
//		}
//		return uint8(v), false, nil
//	case int8:
//		if v < 0 {
//			return uint8(math.MaxUint8 - uint(-v)), true, nil
//		}
//		return uint8(v), false, nil
//	case int32:
//		if v < 0 {
//			return uint8(math.MaxUint8 + uint(v)), true, nil
//		}
//		return uint8(v), false, nil
//	case int64:
//		if v < 0 {
//			return uint8(math.MaxUint8 + uint(v)), true, nil
//		}
//		return uint8(v), false, nil
//	case uint:
//		return uint8(v), false, nil
//	case uint16:
//		return uint8(v), false, nil
//	case uint64:
//		return uint8(v), false, nil
//	case uint32:
//		return uint8(v), false, nil
//	case uint8:
//		return v, false, nil
//	case float32:
//		if v > float32(math.MaxInt8) {
//			return math.MaxUint8, true, nil
//		} else if v < 0 {
//			return uint8(math.MaxUint8 - v), true, nil
//		}
//		return uint8(math.Round(float64(v))), false, nil
//	case float64:
//		if v >= float64(math.MaxUint8) {
//			return math.MaxUint8, true, nil
//		} else if v <= 0 {
//			return uint8(math.MaxUint8 - v), true, nil
//		}
//		return uint8(math.Round(v)), false, nil
//	case decimal.Decimal:
//		if v.GreaterThan(dec_uint8_max) {
//			return math.MaxUint8, false, nil
//		} else if v.LessThan(dec_zero) {
//			ret, _ := dec_uint8_max.Sub(v).Float64()
//			return uint8(math.Round(ret)), true, nil
//		}
//		// TODO: If we ever internally switch to using Decimal for large numbers, this will need to be updated
//		f, _ := v.Float64()
//		return uint8(math.Round(f)), false, nil
//	case []byte:
//		i, err := strconv.ParseUint(hex.EncodeToString(v), 8, 8)
//		if err != nil {
//			return 0, false, sql.ErrInvalidValue.New(v, t.String())
//		}
//		return uint8(i), false, nil
//	case string:
//		i, err := strconv.ParseUint(v, 10, 8)
//		if err != nil {
//			return 0, false, sql.ErrInvalidValue.New(v, t.String())
//		}
//		return uint8(i), false, nil
//	case bool:
//		if v {
//			return 1, false, nil
//		}
//		return 0, false, nil
//	case nil:
//		return 0, false, nil
//	default:
//		return 0, false, sql.ErrInvalidValueType.New(v, t.String())
//	}
//}
