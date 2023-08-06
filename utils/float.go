package utils

import "strconv"

func Float64ToBytes(val float64) []byte {
	return []byte(strconv.FormatFloat(val, 'f', -1, 64))
}

func FloatFromByte(val []byte) (float64, error) {
	return strconv.ParseFloat(string(val), 64)
}
