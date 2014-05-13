package types

import (
	"fmt"
)

// 16 byte value
type B16 struct {
	b0 uint64
	b1 uint64
}

func (b *B16) MarshalBinary() ([]byte, error) {
	bs := make([]byte, 16)
	for i:=uint32(0);i<16;i++ {
		var src *uint64
		var offset uint32
		if i < 8 {
			src = &b.b0
			offset = i
		} else {
			src = &b.b1
			offset = i - 8
		}

		bs[i] = byte((*src >> ((7 - offset) * 8)) & 0xff)
	}
	return bs, nil
}

func (b *B16) UnmarshalBinary(bs []byte) error {
	if len(bs) != 16 {
		return fmt.Errorf("Exactly 16 bytes required for unmarshal, got %v", len(bs))
	}
	for i:=uint32(0);i<16;i++ {
		var dst *uint64
		var offset uint32
		if i < 8 {
			dst = &b.b0
			offset = i
		} else {
			dst = &b.b1
			offset = i - 8
		}
		*dst |= uint64(bs[i]) << ((7 - offset) * 8)
	}
	return nil
}

func (b *B16) IsZero() bool {
	return b.b0 == 0 && b.b1 == 0
}
