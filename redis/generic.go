package redis

import "errors"

func (d *DataStructure) Del(key []byte) error {
	return d.db.Delete(key)
}

func (d *DataStructure) Type(key []byte) (DataType, error) {
	encodedValue, err := d.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encodedValue) == 0 {
		return 0, errors.New("value is nil")
	}
	return encodedValue[0], nil
}
