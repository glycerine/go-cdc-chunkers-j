/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package ultracdc

import (
	"errors"
	"fmt"
	"math/bits"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

func init() {
	chunkers.Register("ultracdc", newUltraCDC)
}

var ErrNormalSize = errors.New("NormalSize is required and must be 64B <= NormalSize <= 1GB")
var ErrMinSize = errors.New("MinSize is required and must be 64B <= MinSize <= 1GB && MinSize < NormalSize")
var ErrMaxSize = errors.New("MaxSize is required and must be 64B <= MaxSize <= 1GB && MaxSize > NormalSize")
var ErrMinSizeNotMultipleOf8 = errors.New("MinSize must be evenly divisible by 8")

type UltraCDC struct {
}

func newUltraCDC() chunkers.ChunkerImplementation {
	return &UltraCDC{}
}

func (c *UltraCDC) DefaultOptions() *chunkers.ChunkerOpts {
	return &chunkers.ChunkerOpts{
		MinSize:    2 * 1024,
		NormalSize: 8 * 1024,
		MaxSize:    64 * 1024,
	}
}

func (c *UltraCDC) Validate(options *chunkers.ChunkerOpts) error {
	if options.MinSize%8 != 0 {
		return ErrMinSizeNotMultipleOf8
	}
	if options.NormalSize == 0 || options.NormalSize < 64 ||
		options.NormalSize > 1024*1024*1024 {
		return ErrNormalSize
	}
	if options.MinSize < 64 || options.MinSize > 1024*1024*1024 ||
		options.MinSize >= options.NormalSize {
		return ErrMinSize
	}
	if options.MaxSize < 64 || options.MaxSize > 1024*1024*1024 ||
		options.MaxSize <= options.NormalSize {
		return ErrMaxSize
	}
	return nil
}

func (c *UltraCDC) Algorithm(options *chunkers.ChunkerOpts, data []byte, n, pass int) int {
	if n > len(data) {
		panic(fmt.Sprintf("len(data) == %v, but must be >= n == %v", len(data), n))
	}

	// isolate unsafe to unsafe.go file.
	var src []uint64 = bytesToUint64(data)

	const (
		Pattern uint64 = 0xAAAAAAAAAAAAAAAA
		MaskS   uint64 = 0x2F
		MaskL   uint64 = 0x2C

		// Low Entropy String Threshold
		LEST uint32 = 64
	)
	MinSize := options.MinSize
	MaxSize := options.MaxSize
	NormalSize := options.NormalSize

	i := MinSize
	cnt := uint32(0)
	mask := MaskS

	switch {
	case n <= MinSize:
		return n
	case n >= MaxSize:
		n = MaxSize
	case n <= NormalSize:
		NormalSize = n
	}

	// k is our index into src, now that src is []uint64
	k := i / 8
	outBufWin := src[k]
	dist := bits.OnesCount64(outBufWin ^ Pattern)
	i += 8
	k++

	for i < n-8 {
		if i == NormalSize {
			mask = MaskL
		}

		if k >= len(src) {
			fmt.Printf("crashing on i = %v; k(%v) >= len(src)=%v; len(data)=%v; n=%v; len(data)/8=%v\n", i, k, len(src), len(data), n, float64(len(data))/8)
			return 0
		}
		inBufWin := src[k]
		if (outBufWin ^ inBufWin) == 0 {
			cnt++
			if cnt == LEST {
				fmt.Printf("on pass = %v, cnt = %v == LEST = %v, returning i+8=%v\n", pass, cnt, LEST, i+8) // never seen??
				return i + 8
			}
			i += 8
			k++
			continue
		}

		cnt = 0
		for j := 0; j < 8; j++ {
			if (uint64(dist) & mask) == 0 {
				fmt.Printf("on pass = %v, dist: %v (%b) & mask: %v (%b) == %v, returning i+8=%v\n", pass, dist, dist, mask, mask, uint64(dist)&mask, i+8)
				return i + 8
			}
			// words:          outBufWin        inBufWin
			// byte index:    [0 1 2 3 4 5 6 7][0 1 2 3 4 5 6 7] if big-endian.
			// byte index:    [7 6 5 4 3 2 1 0][7 6 5 4 3 2 1 0] if little-endian.
			// slide by one:     [               ]
			// means           ^ has to go out; ^ has to go in.

			// little endian:
			inByte := byte(inBufWin >> (j << 3))
			outByte := byte(outBufWin >> (j << 3))
			// big endian: (to view the []byte stream as continuously numbered)
			//inByte := byte(inBufWin >> ((7 - j) << 3))
			//outByte := byte(outBufWin >> ((7 - j) << 3))

			if i+j+8 >= len(data) {
				// crashing when i(56920) + j(3) + 8 = 56931 >= len(data)=56931
				fmt.Printf("crashing when i(%v) + j(%v) + 8 = %v >= len(data)=%v\n", i, j, i+j+8, len(data))
				return 0
			}
			outByte2 := data[i+j-8]
			inByte2 := data[i+j]
			if outByte != outByte2 || inByte != inByte2 {
				fmt.Printf("inByte = %x ; outByte = %x; data[i+j-8]=%x should be outByte; data[i+j]=%x should be in byte; i=%v; j=%v; k=%v; k*8=%v; n=%v; pass=%v; outBufWin='%x'; inBufWin='%x'; outByte2=%x; inByte2=%x\n", inByte, outByte, data[i+j-8], data[i+j], i, j, k, k*8, n, pass, outBufWin, inBufWin, outByte2, inByte2)
				for k, v := range data[(i - 8) : i+j+8+1] {
					fmt.Printf("data[%v] = %x\n", k+i-8, v)
				}
				panic("why not?")
			} else {
				//fmt.Printf("bytes agree at i =%v; j=%v\n", i, j)
			}
			/*
				if debug < 5 {
					fmt.Printf("inByte = %v ; outByte = %v; data[i+j]=%v should be outByte; data[i+j+8]=%v should be in byte; i=%v\n", inByte, outByte, data[i+j], data[i+j+8], i)
				}
				debug++
			*/
			//dist = dist + uint64(hammingDistanceTable[outByte][inByte])
			update := hammingDistanceTable[0xAA][inByte] - hammingDistanceTable[0xAA][outByte]
			//fmt.Printf("on pass = %v, dist: %v -> %v\n", pass, dist, dist+update)
			dist += update
		}
		outBufWin = inBufWin
		i += 8
		k++
	}

	return n
}

var debug int = 0
