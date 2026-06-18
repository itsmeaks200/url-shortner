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
  return str.split('').reduce((acc, c) => acc * 62 + CHARS.indexOf(c), 0);
}

module.exports = { encode, decode };
