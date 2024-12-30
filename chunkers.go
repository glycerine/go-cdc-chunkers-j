package chunkers

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

import (
	"bufio"
	"errors"
	"io"
)

type ChunkerOpts struct {
	MinSize    int
	MaxSize    int
	NormalSize int
}

type ChunkerImplementation interface {
	DefaultOptions() *ChunkerOpts
	Validate(*ChunkerOpts) error
	Algorithm(*ChunkerOpts, []byte, int) int
}

type Chunker struct {
	rd             *bufio.Reader
	options        *ChunkerOpts
	implementation ChunkerImplementation

	cutpoint int

	maxSize    int
	minSize    int
	normalSize int
}

func (c *Chunker) MinSize() int {
	return c.options.MinSize
}

func (c *Chunker) MaxSize() int {
	return c.options.MaxSize
}

func (c *Chunker) NormalSize() int {
	return c.options.NormalSize
}

var chunkers map[string]func() ChunkerImplementation = make(map[string]func() ChunkerImplementation)

func Register(name string, implementation func() ChunkerImplementation) error {
	if _, exists := chunkers[name]; exists {
		return errors.New("algorithm already registered")
	}
	chunkers[name] = implementation
	return nil
}

func NewChunker(algorithm string, reader io.Reader, opts *ChunkerOpts) (*Chunker, error) {
	var implementationAllocator func() ChunkerImplementation

	implementationAllocator, exists := chunkers[algorithm]
	if !exists {
		return nil, errors.New("unknown algorithm")
	}

	if opts == nil {
		opts = implementationAllocator().DefaultOptions()
	}

	chunker := &Chunker{}
	chunker.implementation = implementationAllocator()
	chunker.options = opts
	chunker.rd = bufio.NewReaderSize(reader, int(chunker.options.MaxSize)*2)

	chunker.minSize = chunker.options.MinSize
	chunker.maxSize = chunker.options.MaxSize
	chunker.normalSize = chunker.options.NormalSize

	return chunker, nil
}

func (chunker *Chunker) Next() ([]byte, error) {
	if chunker.cutpoint != 0 {
		// Discard is guaranteed to succeed here, do not check for error
		chunker.rd.Discard(chunker.cutpoint)
		chunker.cutpoint = 0
	}

	data, err := chunker.rd.Peek(chunker.maxSize)
	if err != nil && err != io.EOF {
		return nil, err
	}

	n := len(data)
	if n == 0 {
		return nil, io.EOF
	}

	cutpoint := chunker.implementation.Algorithm(chunker.options, data, n)
	chunker.cutpoint = cutpoint

	if cutpoint < chunker.minSize {
		return data[:cutpoint], io.EOF
	}

	return data[:cutpoint], nil
}

func (chunker *Chunker) Copy(dst io.Writer) (int64, error) {
	nbytes := int64(0)
	for {
		chunk, err := chunker.Next()
		if err != nil && err != io.EOF {
			return nbytes, err
		}

		if len(chunk) != 0 {
			if _, werr := dst.Write(chunk); werr != nil {
				return nbytes, werr
			}
		}
		if err == io.EOF {
			break
		}

		nbytes += int64(len(chunk))
	}
	return nbytes, io.EOF
}

func (chunker *Chunker) Split(callback func(offset, length uint, chunk []byte) error) error {
	offset := uint(0)
	for {
		chunk, err := chunker.Next()
		if err != nil && err != io.EOF {
			return err
		}

		if len(chunk) != 0 {
			if err = callback(offset, uint(len(chunk)), chunk); err != nil {
				return err
			}
		}

		if err == io.EOF {
			break
		}
		offset += uint(len(chunk))
	}
	return nil
}
