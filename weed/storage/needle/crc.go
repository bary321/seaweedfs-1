package needle

import (
	"fmt"

	"github.com/klauspost/crc32"

	"github.com/bary321/seaweedfs-1/weed/util"
)

var table = crc32.MakeTable(crc32.Castagnoli)

type CRC uint32

func NewCRC(b []byte) CRC {
	return CRC(0).Update(b)
}

func (c CRC) Update(b []byte) CRC {
	return CRC(crc32.Update(uint32(c), table, b))
}

func (c CRC) Value() uint32 {
	return uint32(c>>15|c<<17) + 0xa282ead8
}

func (n *Needle) Etag() string {
	bits := make([]byte, 4)
	util.Uint32toBytes(bits, uint32(n.Checksum))
	return fmt.Sprintf("%x", bits)
}
