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
  test('decodes "0" to 0n', () => {
    expect(decode('0')).toBe(0n);
  });

  test('decodes "10" to 62n', () => {
    expect(decode('10')).toBe(62n);
  });

  test('decodes "Z" to 61n', () => {
    expect(decode('Z')).toBe(61n);
  });

  test('rejects invalid characters', () => {
    expect(() => decode('!!!')).toThrow('Invalid base62 character: !');
  });
});

describe('base62 roundtrip', () => {
  const cases = [0n, 1n, 61n, 62n, 100n, 9999n, 123456789n, 339411272711802881n];

  test.each(cases)('decode(encode(%i)) === %i', (n) => {
    expect(decode(encode(n))).toBe(n);
  });
});
