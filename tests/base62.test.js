'use strict';

const { encode, decode } = require('../src/utils/base62');

const CHARS = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';

describe('base62.encode', () => {
  test('encodes 0 to "0"', () => {
    expect(encode(0)).toBe('0');
  });

  test('encodes 61 to "Z" (last single-char value)', () => {
    expect(encode(61)).toBe('Z');
  });

  test('encodes 62 to "10" (first two-char value)', () => {
    expect(encode(62)).toBe('10');
  });

  test('accepts BigInt input', () => {
    expect(encode(62n)).toBe('10');
  });

  test('output contains only valid Base62 characters', () => {
    const result = encode(123456789);
    for (const c of result) {
      expect(CHARS).toContain(c);
    }
  });
});

describe('base62.decode', () => {
  test('decodes "0" to 0', () => {
    expect(decode('0')).toBe(0);
  });

  test('decodes "10" to 62', () => {
    expect(decode('10')).toBe(62);
  });

  test('decodes "Z" to 61', () => {
    expect(decode('Z')).toBe(61);
  });
});

describe('base62 roundtrip', () => {
  const cases = [0, 1, 61, 62, 100, 9999, 123456789];

  test.each(cases)('decode(encode(%i)) === %i', (n) => {
    expect(decode(encode(n))).toBe(n);
  });
});
