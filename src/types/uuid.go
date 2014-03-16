package types

import (
	"fmt"
)

import (
	"code.google.com/p/go-uuid/uuid"
)

// wraps up the google uuid library
// and stores values as 2 uint64 values
type UUID struct {
	u0 uint64
	u1 uint64
}

func (u *UUID) MarshalBinary() ([]byte, error) {
	bs := make([]byte, 16)
	for i:=uint32(0);i<16;i++ {
		var src *uint64
		var offset uint32
		if i < 8 {
			src = &u.u0
			offset = i
		} else {
			src = &u.u1
			offset = i - 8
		}

		bs[i] = byte((*src >> ((7 - offset) * 8)) & 0xff)
	}
	return bs, nil
}

func (u *UUID) UnmarshalBinary(bs []byte) error {
	if len(bs) != 16 {
		return fmt.Errorf("Exactly 16 bytes required for unmarshal, got %v", len(bs))
	}
	for i:=uint32(0);i<16;i++ {
		var dst *uint64
		var offset uint32
		if i < 8 {
			dst = &u.u0
			offset = i
		} else {
			dst = &u.u1
			offset = i - 8
		}
		*dst |= uint64(bs[i]) << ((7 - offset) * 8)
	}
	return nil
}

func NewUUID1() UUID {
	u := &UUID{}
	err := u.UnmarshalBinary([]byte(uuid.NewUUID()))
	if err != nil {
		panic(err)
	}
	return *u
}

func NewUUID4() UUID {
	u := &UUID{}
	err := u.UnmarshalBinary([]byte(uuid.NewRandom()))
	if err != nil {
		panic(err)
	}
	return *u
}

func (u UUID) Time() int64 {
	bs, err := (&u).MarshalBinary()
	if err != nil {
		panic(err)
	}
	uu := uuid.UUID(bs)
	t, _ := uu.Time()
	return int64(t)
}

