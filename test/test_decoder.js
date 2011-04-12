
var assert = require('assert');
var BigInteger = require('../lib/bigint').BigInteger;
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

/** make sure sign extension and unsigned/signed conversions don't bite us. */
exports['testBigIntEdges'] = function() {
  
  assert.ok(new BigInteger([255]).equals(new BigInteger([-1])));
  assert.ok(new BigInteger([245]).equals(new BigInteger([-11])));
  assert.deepEqual(new BigInteger([255]).toByteArray(), new BigInteger([-1]).toByteArray());
  assert.deepEqual(new BigInteger([245]).toByteArray(), new BigInteger([-11]).toByteArray());
  assert.deepEqual(new BigInteger([255]), new BigInteger([-1]));
  assert.deepEqual(new BigInteger([245]), new BigInteger([-11]));
  
};

/** verify byte array fidelity with java.math.BigInteger */
exports['testBigInt'] = function() {
  // these arrays were generated using java program below.
  var expectedArrays = [
    [ 23 ],
    [ 0, -127 ],
    [ 1, 3 ],
    [ 4, 5 ],
    [ 32, 0, 0, 0, 0 ],
    [ 64, 0, 0, 0, 0 ],
    [ 0, -128, 0, 0, 0, 0 ],
    [ 76, 75, 89, -94, 112, -83, 123, -128 ],
    [ 32, 23, -123, 66, -123, 31, -109, -128 ],
    [ 122, -80, -3, 114, -84, 96, 0 ],
    [ 8, 0, 0, 0, 0, 0, 0, 0, 0 ],
    [ 12, 53, 8, 15, -119, 11, -105, 14, -72, 55, -128 ],
    [ 16, 0, 0, 0, 0, 0, 0, 0, 0 ],
    [ 0, -14, -67, -117, 113, -67, 39, -92, 104, -1, -84, 60 ],
    [ -23 ],
    [ -1, 127 ],
    [ -2, -3 ],
    [ -5, -5 ],
    [ -32, 0, 0, 0, 0 ],
    [ -64, 0, 0, 0, 0 ],
    [ -128, 0, 0, 0, 0 ],
    [ -77, -76, -90, 93, -113, 82, -124, -128 ],
    [ -33, -24, 122, -67, 122, -32, 108, -128 ],
    [ -123, 79, 2, -115, 83, -96, 0 ],
    [ -8, 0, 0, 0, 0, 0, 0, 0, 0 ],
    [ -13, -54, -9, -16, 118, -12, 104, -15, 71, -56, -128 ],
    [ -16, 0, 0, 0, 0, 0, 0, 0, 0 ],
    [ -1, 13, 66, 116, -114, 66, -40, 91, -105, 0, 83, -60 ]
  ];
  var nums = [
    '23',        
    '129',       
    '259',       
    '1029',      
    '137438953472',
    '274877906944',
    '549755813888',
    '5497586324345813888',
    '2312463454425813888',
    '34534549755813888',
    '147573952589676412928',
    '14757543952358956762412928',
    '295147905179352825856',
    '293455147905179352825834556',
                                  
    '-23',       
    '-129',      
    '-259',      
    '-1029',     
    '-137438953472',
    '-274877906944',
    '-549755813888',
    '-5497586324345813888',
    '-2312463454425813888',
    '-34534549755813888',
    '-147573952589676412928',
    '-14757543952358956762412928',
    '-295147905179352825856',
    '-293455147905179352825834556'
  ];
  assert.equal(expectedArrays.length, nums.length);
  for (var i = 0; i < nums.length; i++) {
    assert.deepEqual(new BigInteger(nums[i]).toByteArray(), expectedArrays[i]);
    assert.deepEqual(new BigInteger(nums[i]).toByteArray(), new BigInteger(expectedArrays[i]).toByteArray());
  }
/**
The expected values were all generated from this program:
 
import java.math.BigInteger;
 
public class TestBigInt {
  private static final String[] ints = {
    "23",        
    "129",       
    "259",       
    "1029",      
    "137438953472",
    "274877906944",
    "549755813888",
    "5497586324345813888",
    "2312463454425813888",
    "34534549755813888",
    "147573952589676412928",
    "14757543952358956762412928",
    "295147905179352825856",
    "293455147905179352825834556",
                                  
    "-23",       
    "-129",      
    "-259",      
    "-1029",     
    "-137438953472",
    "-274877906944",
    "-549755813888",
    "-5497586324345813888",
    "-2312463454425813888",
    "-34534549755813888",
    "-147573952589676412928",
    "-14757543952358956762412928",
    "-295147905179352825856",
    "-293455147905179352825834556"
  };
  
  public static void main(String args[]) {
    for (String s : ints)
      System.out.println(toString(new BigInteger(s).toByteArray()));
  }
  
  private static String toString(byte[] arr) {
    StringBuilder sb = new StringBuilder("[ ");
    for (byte b : arr) {
      sb.append((int)b).append(", ");
    }
    return sb.toString().substring(0, sb.length()-2) + " ]";
  }
}
 */
};

exports.testBigIntEdges();