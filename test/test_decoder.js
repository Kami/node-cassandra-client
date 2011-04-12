
var assert = require('assert');
var bytesToLong = require('../lib/decoder').bytesToLong;

exports['testLongConversion'] = function() {
  assert.ok(bytesToLong);
  assert.equal(1, bytesToLong('\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0001')); // 1
  assert.equal(2, bytesToLong('\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0002')); // 2
  assert.equal(255 ,bytesToLong('\u0000\u0000\u0000\u0000\u0000\u0000\u0000ÿ')); // 255
  assert.equal(2550 ,bytesToLong('\u0000\u0000\u0000\u0000\u0000\u0000\tö')); // 2550
  assert.equal(8025521, bytesToLong('\u0000\u0000\u0000\u0000\u0000zu±')); // 8025521
  assert.equal(218025521, bytesToLong('\u0000\u0000\u0000\u0000\fþÎ1')); // 218025521
  assert.equal(-1312133133, bytesToLong('\u0000\u0000\u0005ó±Ên1')); // 6544218025521 unsigned
  assert.equal(-68262595, bytesToLong('yÌa\u001c²be1')); // 8776496549718025521 unsigned
};