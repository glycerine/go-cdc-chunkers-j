/*
 * Copyright (c) 2024 Gilles Chehade <gilles@poolp.org>
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
	"reflect"
	"unsafe"
)

// cast to []uint64, keeping the underlying storage the same.
// This assumes little-endian byte order.
func bytesToUint64(b []byte) []uint64 {
	// Ensure byte slice length is a multiple of 8 (size of uint64)
	n := len(b)
	if n < 8 {
		return nil
	}
	remainder := n % 8
	if remainder != 0 {
		n -= remainder
		b = b[:n] // ignore the last few bytes.
	}

	// Get the header of the byte slice
	header := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	// Create a new slice header for uint64
	uint64Slice := &reflect.SliceHeader{
		Data: header.Data,
		Len:  header.Len / 8, // Each uint64 is 8 bytes
		Cap:  header.Cap / 8,
	}

	// Convert the header to []uint64
	return *(*[]uint64)(unsafe.Pointer(uint64Slice))
}
