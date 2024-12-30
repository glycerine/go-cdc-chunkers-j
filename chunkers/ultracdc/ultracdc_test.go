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
	"encoding/hex"
	"fmt"
	mathrand2 "math/rand/v2"
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	"lukechampine.com/blake3"
	//"github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
)

// Sanity check that refactoring ultracdc.go has
// not changed its output.
func Test_Splits_Not_Changed(t *testing.T) {

	// deterministic pseudo-random numbers as data.
	var seed [32]byte
	generator := mathrand2.NewChaCha8(seed)
	data := make([]byte, 1<<20+1)
	generator.Read(data)

	//data = append([]byte{0x39, 0x46}, data...)
	u := newUltraCDC().(*UltraCDC)
	//u := &fastcdc.FastCDC{}
	opt := u.DefaultOptions()
	cuts, hashmap := getCuts(data, u, opt)

	/*
		for j, cut := range cuts {
			if expectedCuts[j] != cut {
				t.Fatalf(`expected %v but got %v at j = %v`, expectedCuts[j], cut, j)
			}
		}
	*/

	// how many change if we alter the data? just by prepending 2 bytes.
	differ := 0
	data = append([]byte{0x39, 0x46}, data...)
	cuts2, hashmap2 := getCuts(data, u, opt)
	for j, cut := range cuts2 {
		if cuts[j] != cut {
			differ++
			//fmt.Printf("cut %v differs: %v vs %v   (off by %v)\n", j, cut, cuts[j], cut-cuts[j])
		}
	}
	fmt.Printf("after pre-pending 2 bytes, the number of cuts that differ = %v; out of %v\n", differ, len(cuts))

	matchingHashes := 0
	for hash0 := range hashmap {
		if hashmap2[hash0] {
			matchingHashes++
		}
	}
	fmt.Printf("matchingHashes = %v\n", matchingHashes)
}

func getCuts(data []byte, u chunkers.ChunkerImplementation, opt *chunkers.ChunkerOpts) (cuts []int, hashmap map[string]bool) {

	hashmap = make(map[string]bool)
	last := 0
	j := 0
	for len(data) > opt.MinSize {
		offset := u.Algorithm(opt, data, len(data))
		if offset == 0 {
			break
		}
		cut := last + offset
		cuts = append(cuts, cut)
		last = cut
		j++
		hashmap[Blake3OfBytes(data[:offset])] = true
		data = data[offset:]
	}
	return
}

func regenExpected() {
	// deterministic pseudo-random numbers as data.
	var seed [32]byte
	generator := mathrand2.NewChaCha8(seed)
	data := make([]byte, 1<<20+1)
	generator.Read(data)

	u := newUltraCDC()
	opt := u.DefaultOptions()
	var cuts []int
	last := 0
	j := 0
	fmt.Printf("var expectedCuts = []int{\n")
	for len(data) > opt.MinSize {
		offset := u.Algorithm(opt, data, len(data))
		cut := last + offset
		cuts = append(cuts, cut)
		last = cut
		fmt.Printf("%v, ", cut)
		j++
		if j%8 == 0 {
			fmt.Println()
		}
		data = data[offset:]
	}
	fmt.Printf("\n}\n")
}

var expectedCuts = []int{
	2104, 4224, 6288, 8384, 10456, 12608, 14688, 16760,
	18832, 20904, 22976, 25072, 27160, 29264, 31352, 33480,
	35552, 37624, 39720, 41808, 43872, 45944, 48072, 50152,
	52288, 54360, 56464, 58584, 60648, 62760, 64896, 67032,
	69120, 71208, 73280, 75424, 77496, 79568, 81640, 83744,
	85816, 87888, 89952, 92016, 94088, 96192, 98256, 100328,
	102416, 104504, 106584, 108656, 110728, 112848, 114936, 117008,
	119144, 121264, 123400, 125480, 127552, 129624, 131688, 133776,
	135928, 138000, 140136, 142216, 144320, 146552, 148624, 150712,
	152800, 154864, 156936, 159008, 161088, 163160, 165232, 167312,
	169400, 171464, 173632, 175704, 177800, 179872, 181952, 184040,
	186112, 188184, 190256, 192328, 194424, 196544, 198648, 200744,
	202832, 205000, 207072, 209264, 211456, 213528, 215600, 217680,
	219752, 221840, 223928, 226000, 228064, 230136, 232224, 234296,
	236368, 238472, 240600, 242672, 244744, 246824, 248896, 251008,
	253096, 255248, 257360, 259432, 261504, 263576, 265704, 267880,
	269968, 272088, 274152, 276312, 278384, 280568, 282656, 284728,
	286800, 288944, 291048, 293112, 295184, 297304, 299376, 301472,
	303544, 305616, 307688, 309760, 311848, 313952, 316024, 318096,
	320184, 322256, 324328, 326416, 328504, 330576, 332680, 334752,
	336824, 338896, 340968, 343040, 345104, 347248, 349384, 351456,
	353536, 355608, 357712, 359784, 361856, 363928, 366000, 368080,
	370144, 372216, 374288, 376376, 378464, 380568, 382640, 384712,
	386784, 388872, 390944, 393016, 395104, 397176, 399248, 401320,
	403408, 405480, 407544, 409632, 411704, 413792, 415912, 417984,
	420088, 422152, 424224, 426328, 428416, 430480, 432568, 434640,
	436760, 438832, 440904, 442976, 445064, 447136, 449224, 451312,
	453384, 455448, 457560, 459632, 461720, 463792, 465856, 467928,
	470008, 472128, 474192, 476264, 478336, 480408, 482480, 484552,
	486624, 488720, 490792, 492888, 494992, 497064, 499152, 501256,
	503344, 505416, 507488, 509584, 511656, 513736, 515848, 517928,
	520000, 522072, 524136, 526224, 528336, 530472, 532544, 534632,
	536720, 538784, 540856, 542928, 545072, 547176, 549248, 551320,
	553392, 555464, 557536, 559608, 561680, 563768, 565856, 567960,
	570032, 572104, 574176, 576256, 578352, 580424, 582600, 584824,
	586944, 589032, 591104, 593192, 595256, 597344, 599416, 601520,
	603584, 605656, 607728, 609816, 611912, 613976, 616080, 618160,
	620248, 622320, 624408, 626496, 628560, 630680, 632752, 634824,
	636912, 639032, 641104, 643184, 645288, 647360, 649480, 651600,
	653672, 655744, 657808, 659992, 662136, 664224, 666344, 668432,
	670504, 672608, 674680, 676792, 678888, 680960, 683032, 685104,
	687176, 689264, 691336, 693456, 695544, 697632, 699704, 701784,
	703856, 706032, 708120, 710208, 712336, 714408, 716496, 718584,
	720656, 722728, 724800, 726872, 728968, 731072, 733160, 735256,
	737328, 739400, 741600, 743704, 745792, 747880, 750016, 752088,
	754176, 756296, 758368, 760448, 762520, 764592, 766680, 768752,
	770872, 772944, 775016, 777088, 779200, 781296, 783424, 785544,
	787632, 789768, 791840, 793944, 796144, 798232, 800320, 802400,
	804520, 806584, 808672, 810760, 812872, 814976, 817048, 819120,
	821184, 823264, 825336, 827440, 829504, 831576, 833648, 835720,
	837792, 839864, 842016, 844096, 846176, 848288, 850376, 852472,
	854560, 856664, 858736, 860840, 862912, 864976, 867048, 869136,
	871208, 873280, 875384, 877472, 879552, 881624, 883696, 885768,
	887872, 889960, 892032, 894168, 896272, 898400, 900488, 902560,
	904656, 906728, 908856, 910944, 913048, 915136, 917264, 919424,
	921496, 923592, 925664, 927752, 929816, 931936, 934008, 936096,
	938168, 940256, 942360, 944464, 946552, 948640, 950728, 952896,
	954968, 957088, 959160, 961232, 963400, 965488, 967560, 969648,
	971720, 973792, 975872, 977944, 980048, 982184, 984256, 986328,
	988400, 990488, 992584, 994720, 996792, 998864, 1000952, 1003024,
	1005088, 1007208, 1009272, 1011344, 1013416, 1015496, 1017568, 1019640,
	1021704, 1023768, 1025856, 1027920, 1029984, 1032088, 1034160, 1036232,
	1038368, 1040432, 1042560, 1044632, 1046728,
}

// big endian
func getByte(x uint64, i int) byte {
	// Method 1: Using bit shifting
	return byte(x >> ((7 - i) << 3))

	// Method 2: Using bit masking
	// return byte((x >> ((7 - i) * 8)) & 0xFF)
}

/*
func TestByteGet(t *testing.T) {
	x := uint64(0x1122334455667788)

	// little endian access will print:
	// Byte 0: 88
	// Byte 1: 77
	// Byte 2: 66
	// Byte 3: 55
	// Byte 4: 44
	// Byte 5: 33
	// Byte 6: 22
	// Byte 7: 11
	for i := 0; i < 8; i++ {
		fmt.Printf("Byte %d: %02x\n", i, getByte(x, i))
	}
}
*/

func Blake3OfBytes(by []byte) string {
	h := blake3.New(64, nil)
	h.Write(by)
	//return h.Sum(nil)
	enchex := hex.EncodeToString(h.Sum(nil))
	return enchex
}
