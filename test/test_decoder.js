/*
 *  Copyright 2011 Rackspace
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */


var BigInteger = require('../lib/bigint').BigInteger;
var bytesToLong = require('../lib/decoder').bytesToLong;

// friggen big integer library is broken.
exports.testBigIntegerBrokenness = function(test, assert) {
  // the the v8 lib behaved like the java BigInteger, this test would pass.
  
  // this is how the zeroes we read out of the database get constructed.
  var zero1 = new BigInteger([0]);
  assert.ok(zero1);
  
  // this is how zeroes will be constructed by programmers.
  var zero2 = new BigInteger('0');
  assert.ok(zero2);
  
  // notice: they are not equal, but if the v8 lib _really_ behaved like java.math.BigInteger (which it claims to emulate)
  // equality should be achived.
  assert.ok(!zero1.equals(zero2));
  
  // instead, we need to rely on bytesToLong to return the right thing.
  assert.ok(zero2.equals(bytesToLong('\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000')));
  
  test.finish();
};

exports.testLongConversion = function(test, assert) {
  assert.ok(bytesToLong);
  // we have to compare against strings.
  assert.ok(new BigInteger('0').equals(bytesToLong('\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000')));
  assert.strictEqual('0', bytesToLong('\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000').toString()); // 1
  assert.strictEqual('1', bytesToLong('\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0001').toString()); // 1
  assert.strictEqual('2', bytesToLong('\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0002').toString()); // 2
  assert.strictEqual('255' ,bytesToLong('\u0000\u0000\u0000\u0000\u0000\u0000\u0000ÿ').toString()); // 255
  assert.strictEqual('2550' ,bytesToLong('\u0000\u0000\u0000\u0000\u0000\u0000\tö').toString()); // 2550
  assert.strictEqual('8025521', bytesToLong('\u0000\u0000\u0000\u0000\u0000zu±').toString()); // 8025521
  assert.strictEqual('218025521', bytesToLong('\u0000\u0000\u0000\u0000\fþÎ1').toString()); // 218025521
  assert.strictEqual('6544218025521', bytesToLong('\u0000\u0000\u0005ó±Ên1').toString()); // 6544218025521
  assert.strictEqual('8776496549718025521', bytesToLong('yÌa\u001c²be1').toString()); // 8776496549718025521
  
  test.finish();
};

/** make sure sign extension and unsigned/signed conversions don't bite us. */
exports.testBigIntEdges = function(test, assert) {
  
  assert.ok(new BigInteger([255]).equals(new BigInteger([-1])));
  assert.ok(new BigInteger([245]).equals(new BigInteger([-11])));
  assert.deepEqual(new BigInteger([255]).toByteArray(), new BigInteger([-1]).toByteArray());
  assert.deepEqual(new BigInteger([245]).toByteArray(), new BigInteger([-11]).toByteArray());
  assert.deepEqual(new BigInteger([255]), new BigInteger([-1]));
  assert.deepEqual(new BigInteger([245]), new BigInteger([-11]));
  
  test.finish();
};

/** verify byte array fidelity with java.math.BigInteger */
exports.testBigInt = function(test, assert) {
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
  
  test.finish();
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

exports.testUUID = function(test, assert) {
  /* from java:
  ddf09190-6612-11e0-0000-fe8ebeead9f8->[221,240,145,144,102,18,17,224,0,0,254,142,190,234,217,248,] 
  ddf0b8a0-6612-11e0-0000-1e4e5d5425fc->[221,240,184,160,102,18,17,224,0,0,30,78,93,84,37,252,]      
  ddf0b8a1-6612-11e0-0000-90f061abd1ff->[221,240,184,161,102,18,17,224,0,0,144,240,97,171,209,255,]  
   */
  var strings = ['ddf09190-6612-11e0-0000-fe8ebeead9f8',
                 'ddf0b8a0-6612-11e0-0000-1e4e5d5425fc',
                 'ddf0b8a1-6612-11e0-0000-90f061abd1ff'];
  var arrays = [[221,240,145,144,102,18,17,224,0,0,254,142,190,234,217,248], 
                [221,240,184,160,102,18,17,224,0,0,30,78,93,84,37,252],
                [221,240,184,161,102,18,17,224,0,0,144,240,97,171,209,255]];

  assert.strictEqual(strings.length, arrays.length);
  for (var i = 0; i < strings.length; i++) {
    assert.deepEqual(new UUID('string', strings[i]).bytes, arrays[i]);
  }
  test.finish();
};