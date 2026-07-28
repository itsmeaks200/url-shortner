'use strict';

const CHARS = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';

function encode(num) {
  let result = '';
  let n = BigInt(num);
  const base = 62n;
  if (n === 0n) return '0';
  while (n > 0n) {
    result = CHARS[Number(n % base)] + result;
    n = n / base;
  }
  return result;
}

function decode(str) {
  let result = 0n;

  for (const c of str) {
    const index = CHARS.indexOf(c);
    if (index === -1) {
      throw new Error(`Invalid base62 character: ${c}`);
    }

    result = result * 62n + BigInt(index);
  }

  return result;
}

module.exports = { encode, decode };
