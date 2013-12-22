package cluster

import (
	"bufio"
	"bytes"
	"testing"
	"time"
)

func TestTimeEncodeDecode(t *testing.T) {
	buf := &bytes.Buffer{}
	src := time.Now()

	writer := bufio.NewWriter(buf)
	if err := writeTime(writer, src); err != nil {
		t.Fatalf("Error writing time: %v", err)
	}
	writer.Flush()

	dst, err := readTime(bufio.NewReader(buf))
	if err != nil {
		t.Fatalf("Error reading time: %v", err)
	}

	if !src.Equal(dst) {
		t.Errorf("Times do not match, expected: %v, got %v", src, dst)
	}
}
