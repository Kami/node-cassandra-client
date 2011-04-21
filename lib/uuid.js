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

/** 
 * Get your wheaties at http://www.ietf.org/rfc/rfc4122.txt
 * Right now this code doesn't distinguish between UUID types.
 **/

/** @data is a stringified type 1 uuid. convert it to an array of ints. */
function stringToBytes(data) {
  var parts = data.split('-');
  var ints = [];
  var intPos = 0;
  for (var i = 0; i < parts.length; i++) {
    for (var j = 0; j < parts[i].length; j+=2) {
      ints[intPos++] = parseInt(parts[i].substr(j, 2), 16);
    }
  }
  return ints;
}

/** @ints is an array of integers. convert to a stringified uuid. */
function bytesToString(ints) {
  var str = '';
  var pos = 0;
  var parts = [4, 2, 2, 2, 6];
  for (var i = 0; i < parts.length; i++) {
    for (var j = 0; j < parts[i]; j++) {
      var octet = ints[pos++].toString(16);
      if (octet.length == 1) {
        octet = '0' + octet;
      }
      str += octet;
    }
    if (parts[i] !== 6) {
      str += '-';
    }
  }
  return str;
}

/** @binaryString is a string of bytes.  this is how binary data comes to us from cassandra. */
function makeInts(binaryString) {
  var ints = [];
  for (var i = 0; i < binaryString.length; i++) {
    ints[i] = binaryString.charCodeAt(i);
    if (ints[i] > 255 || ints[i] < 0) {
      throw new Error('Unexpected byte in binary data.');
    }
  }
  return ints;
}


/**
 * fmt is either 'binary' or 'string'. 'binary' means data is a 16-byte packed string. 'string' means data is a 
 * 4-2-2-2-6 byte dash-delimted stringified uuid (e.g.: 04c3c390-65fb-11e0-0000-7fd66bb03abf).
 */
UUID = module.exports.UUID = function(fmt, data) {
  // stored msb first. each byte is 8 bits of data. there are 16 of em.
  this.bytes = [];
  this.str = '';
  
  if (fmt === 'string') {
    this.str = data;
    this.bytes = stringToBytes(data);
  } else if (fmt === 'binary') {
    this.bytes = makeInts(data);
    this.str = bytesToString(this.bytes);
  } else {
    throw new Error('invalid format: ' + fmt);
  }
};

UUID.prototype.equals = function(uuid) {
  // todo: how do I assert uuid is a UUID?
  if (!uuid.str) {
    return false;
  } else {
    return uuid.str === this.str;
  }
};

UUID.prototype.toString = function() {
  return this.str;
};

// todo: decide what methods need to be exposed. reading/writing only needs a constructor that works. timestamp, node
// and clockSequence are the main ones that come to mind though.