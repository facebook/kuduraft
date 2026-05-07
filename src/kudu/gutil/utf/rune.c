/*
 * The authors of this software are Rob Pike and Ken Thompson.
 *              Copyright (c) 2002 by Lucent Technologies.
 * Permission to use, copy, modify, and distribute this software for any
 * purpose without fee is hereby granted, provided that this entire notice
 * is included in all copies of any software which is or includes a copy
 * or modification of this software and in all copies of the supporting
 * documentation for such software.
 * THIS SOFTWARE IS BEING PROVIDED "AS IS", WITHOUT ANY EXPRESS OR IMPLIED
 * WARRANTY.  IN PARTICULAR, NEITHER THE AUTHORS NOR LUCENT TECHNOLOGIES MAKE
 * ANY REPRESENTATION OR WARRANTY OF ANY KIND CONCERNING THE MERCHANTABILITY OF
 * THIS SOFTWARE OR ITS FITNESS FOR ANY PARTICULAR PURPOSE.
 */
#include "kudu/gutil/utf/utf.h"
#include "kudu/gutil/utf/utfdef.h"

enum {
  kBit1 = 7,
  kBitx = 6,
  kBit2 = 5,
  kBit3 = 4,
  kBit4 = 3,
  kBit5 = 2,

  kT1 = ((1 << (kBit1 + 1)) - 1) ^ 0xFF, /* 0000 0000 */
  kTx = ((1 << (kBitx + 1)) - 1) ^ 0xFF, /* 1000 0000 */
  kT2 = ((1 << (kBit2 + 1)) - 1) ^ 0xFF, /* 1100 0000 */
  kT3 = ((1 << (kBit3 + 1)) - 1) ^ 0xFF, /* 1110 0000 */
  kT4 = ((1 << (kBit4 + 1)) - 1) ^ 0xFF, /* 1111 0000 */
  kT5 = ((1 << (kBit5 + 1)) - 1) ^ 0xFF, /* 1111 1000 */

  kRune1 = (1 << (kBit1 + 0 * kBitx)) - 1, /* 0000 0000 0111 1111 */
  kRune2 = (1 << (kBit2 + 1 * kBitx)) - 1, /* 0000 0111 1111 1111 */
  kRune3 = (1 << (kBit3 + 2 * kBitx)) - 1, /* 1111 1111 1111 1111 */
  kRune4 = (1 << (kBit4 + 3 * kBitx)) - 1,
  /* 0001 1111 1111 1111 1111 1111 */

  kMaskx = (1 << kBitx) - 1, /* 0011 1111 */
  kTestx = kMaskx ^ 0xFF, /* 1100 0000 */

  kBad = Runeerror,
};

/*
 * Modified by Wei-Hwa Huang, Google Inc., on 2004-09-24
 * This is a slower but "safe" version of the old chartorune
 * that works on strings that are not necessarily null-terminated.
 *
 * If you know for sure that your string is null-terminated,
 * chartorune will be a bit faster.
 *
 * It is guaranteed not to attempt to access "length"
 * past the incoming pointer.  This is to avoid
 * possible access violations.  If the string appears to be
 * well-formed but incomplete (i.e., to get the whole Rune
 * we'd need to read past str+length) then we'll set the Rune
 * to Bad and return 0.
 *
 * Note that if we have decoding problems for other
 * reasons, we return 1 instead of 0.
 */
int charntorune(Rune* rune, const char* str, int length) {
  int c, c1, c2, c3;
  long l;

  /* When we're not allowed to read anything */
  if (length <= 0) {
    goto badlen;
  }

  /*
   * one character sequence (7-bit value)
   *	00000-0007F => T1
   */
  c = *(uchar*)str;
  if (c < kTx) {
    *rune = c;
    return 1;
  }

  // If we can't read more than one character we must stop
  if (length <= 1) {
    goto badlen;
  }

  /*
   * two character sequence (11-bit value)
   *	0080-07FF => T2 Tx
   */
  c1 = *(uchar*)(str + 1) ^ kTx;
  if (c1 & kTestx) {
    goto bad;
  }
  if (c < kT3) {
    if (c < kT2) {
      goto bad;
    }
    l = ((c << kBitx) | c1) & kRune2;
    if (l <= kRune1) {
      goto bad;
    }
    *rune = l;
    return 2;
  }

  // If we can't read more than two characters we must stop
  if (length <= 2) {
    goto badlen;
  }

  /*
   * three character sequence (16-bit value)
   *	0800-FFFF => T3 Tx Tx
   */
  c2 = *(uchar*)(str + 2) ^ kTx;
  if (c2 & kTestx) {
    goto bad;
  }
  if (c < kT4) {
    l = ((((c << kBitx) | c1) << kBitx) | c2) & kRune3;
    if (l <= kRune2) {
      goto bad;
    }
    *rune = l;
    return 3;
  }

  if (length <= 3) {
    goto badlen;
  }

  /*
   * four character sequence (21-bit value)
   *	10000-1FFFFF => T4 Tx Tx Tx
   */
  c3 = *(uchar*)(str + 3) ^ kTx;
  if (c3 & kTestx) {
    goto bad;
  }
  if (c < kT5) {
    l = ((((((c << kBitx) | c1) << kBitx) | c2) << kBitx) | c3) & kRune4;
    if (l <= kRune3) {
      goto bad;
    }
    *rune = l;
    return 4;
  }

  // Support for 5-byte or longer UTF-8 would go here, but
  // since we don't have that, we'll just fall through to bad.

  /*
   * bad decoding
   */
bad:
  *rune = kBad;
  return 1;
badlen:
  *rune = kBad;
  return 0;
}

/*
 * This is the older "unsafe" version, which works fine on
 * null-terminated strings.
 */
int chartorune(Rune* rune, const char* str) {
  int c, c1, c2, c3;
  long l;

  /*
   * one character sequence
   *	00000-0007F => T1
   */
  c = *(uchar*)str;
  if (c < kTx) {
    *rune = c;
    return 1;
  }

  /*
   * two character sequence
   *	0080-07FF => T2 Tx
   */
  c1 = *(uchar*)(str + 1) ^ kTx;
  if (c1 & kTestx) {
    goto bad;
  }
  if (c < kT3) {
    if (c < kT2) {
      goto bad;
    }
    l = ((c << kBitx) | c1) & kRune2;
    if (l <= kRune1) {
      goto bad;
    }
    *rune = l;
    return 2;
  }

  /*
   * three character sequence
   *	0800-FFFF => T3 Tx Tx
   */
  c2 = *(uchar*)(str + 2) ^ kTx;
  if (c2 & kTestx) {
    goto bad;
  }
  if (c < kT4) {
    l = ((((c << kBitx) | c1) << kBitx) | c2) & kRune3;
    if (l <= kRune2) {
      goto bad;
    }
    *rune = l;
    return 3;
  }

  /*
   * four character sequence (21-bit value)
   *	10000-1FFFFF => T4 Tx Tx Tx
   */
  c3 = *(uchar*)(str + 3) ^ kTx;
  if (c3 & kTestx) {
    goto bad;
  }
  if (c < kT5) {
    l = ((((((c << kBitx) | c1) << kBitx) | c2) << kBitx) | c3) & kRune4;
    if (l <= kRune3) {
      goto bad;
    }
    *rune = l;
    return 4;
  }

  /*
   * Support for 5-byte or longer UTF-8 would go here, but
   * since we don't have that, we'll just fall through to bad.
   */

  /*
   * bad decoding
   */
bad:
  *rune = kBad;
  return 1;
}

int isvalidcharntorune(const char* str, int length, Rune* rune, int* consumed) {
  *consumed = charntorune(rune, str, length);
  return *rune != Runeerror || *consumed == 3;
}

int runetochar(char* str, const Rune* rune) {
  /* Runes are signed, so convert to unsigned for range check. */
  unsigned long c;

  /*
   * one character sequence
   *	00000-0007F => 00-7F
   */
  c = *rune;
  if (c <= kRune1) {
    str[0] = c;
    return 1;
  }

  /*
   * two character sequence
   *	0080-07FF => T2 Tx
   */
  if (c <= kRune2) {
    str[0] = kT2 | (c >> 1 * kBitx);
    str[1] = kTx | (c & kMaskx);
    return 2;
  }

  /*
   * If the Rune is out of range, convert it to the error rune.
   * Do this test here because the error rune encodes to three bytes.
   * Doing it earlier would duplicate work, since an out of range
   * Rune wouldn't have fit in one or two bytes.
   */
  if (c > Runemax) {
    c = Runeerror;
  }

  /*
   * three character sequence
   *	0800-FFFF => T3 Tx Tx
   */
  if (c <= kRune3) {
    str[0] = kT3 | (c >> 2 * kBitx);
    str[1] = kTx | ((c >> 1 * kBitx) & kMaskx);
    str[2] = kTx | (c & kMaskx);
    return 3;
  }

  /*
   * four character sequence (21-bit value)
   *     10000-1FFFFF => T4 Tx Tx Tx
   */
  str[0] = kT4 | (c >> 3 * kBitx);
  str[1] = kTx | ((c >> 2 * kBitx) & kMaskx);
  str[2] = kTx | ((c >> 1 * kBitx) & kMaskx);
  str[3] = kTx | (c & kMaskx);
  return 4;
}

int runelen(Rune rune) {
  char str[10];

  return runetochar(str, &rune);
}

int runenlen(const Rune* r, int nrune) {
  int nb, c;

  nb = 0;
  while (nrune--) {
    c = *r++;
    if (c <= kRune1) {
      nb++;
    } else if (c <= kRune2) {
      nb += 2;
    } else if (c <= kRune3) {
      nb += 3;
    } else { /* assert(c <= kRune4) */
      nb += 4;
    }
  }
  return nb;
}

int fullrune(const char* str, int n) {
  if (n > 0) {
    int c = *(uchar*)str;
    if (c < kTx) {
      return 1;
    }
    if (n > 1) {
      if (c < kT3) {
        return 1;
      }
      if (n > 2) {
        if (c < kT4 || n > 3) {
          return 1;
        }
      }
    }
  }
  return 0;
}
