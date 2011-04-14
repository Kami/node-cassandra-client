
/** [en|de]coder for cassandra types. */

var console = require('console');
var BigInteger = require('./bigint').BigInteger;
var UUID = require('./uuid').UUID;

// after this point all precision bets are off.  the carriage becomes a pumpkin and you will lose your glass slipper.
var south = 9007199254740992;

// exported to expose in tests.

/** convert an 8 byte string to a BigInteger */
bytesToLong = module.exports.bytesToLong = function(bytes) {
  if (bytes.length != 8) {
    throw new Error('Longs are exactly 8 bytes, not ' + bytes.length);
  }
  
  return bytesToInt(bytes);
};

bytesToInt = module.exports.bytesToInt = function(bytes) {
  // convert bytes (which is really a string) to a list of ints. then convert the bytes (ints) to a big integer.
  var ints = [];
  for (var i = 0; i < bytes.length; i++) {
    ints[i] = bytes.charCodeAt(i); // how does this handle negative values (bytes that overflow 127)?
    if (ints[i] > 255) {
      throw new Error('Invalid character in packed string ' + ints[i]);
    }
  }
  return new BigInteger(ints);  
};

// These are the cassandra types I'm currently dealing with.
var AbstractTypes = {
  LongType:     'org.apache.cassandra.db.marshal.LongType',  
  BytesType:    'org.apache.cassandra.db.marshal.BytesType',
  AsciiType:    'org.apache.cassandra.db.marshal.AsciiType',
  UTF8Type:     'org.apache.cassandra.db.marshal.UTF8Type',
  IntegerType:  'org.apache.cassandra.db.marshal.IntegerType',
  TimeUUIDType: 'org.apache.cassandra.db.marshal.TimeUUIDType'
};

/** 
 * validators are a hash currently created in the Connection constructor. keys in the hash are: key, comparator, 
 * defaultValidator, specificValidator.  They all map to a value in AbstractTypes, except specificValidator which
 * hashes to another map that maps specific column names to their validators (specified in ColumnDef using Cassandra
 * parlance).
 * e.g.: {key: 'org.apache.cassandra.db.marshal.BytesType', 
 *        comparator: 'org.apache.cassandra.db.marshal.BytesType', 
 *        defaultValidator: 'org.apache.cassandra.db.marshal.BytesType', 
 *        specificValidator: {your_mother: 'org.apache.cassandra.db.marshal.BytesType',
 *                            my_mother: 'org.apache.cassandra.db.marshal.BytesType'}}
 * todo: maybe this is complicated enough that a class is required.
 */
Decoder = module.exports.Decoder = function(validators) {
  this.validators = validators;
};

/**
 * @param bytes raw bytes to decode.
 * @param which one of 'key', 'comparator', or 'value'.
 * @param column (optional) when which is 'value' this parameter specifies which column validator is to be used.
 */
Decoder.prototype.decode = function(bytes, which, column) {
  // determine which type we are converting to.
  var className = null;
  if (which == 'key') {
    className = this.validators.key;
  } else if (which == 'comparator') {
    className = this.validators.comparator;
  } else if (which == 'validator') {
    if (column && this.validators.specificValidators[column]) {
      className = this.validators.specificValidators[column];
    } else {
      className = this.validators.defaultValidator;
    }
  }
  if (!className) {
    console.log('using default for ' + which + ',' + column);
    className = AbstractTypes.BytesType;
  }
  
  // perform the conversion.
  if (className == AbstractTypes.LongType) {
    return bytesToLong(bytes);
  } else if (className == AbstractTypes.AsciiType || className == AbstractTypes.UTF8Type) {
    return bytes; // already as a string!
  } else if (className == AbstractTypes.BytesType) {
    return bytes;
  } else if (className == AbstractTypes.IntegerType) {
    return bytesToInt(bytes);
  } else if (className == AbstractTypes.TimeUUIDType) {
    return new UUID('binary', bytes);
  } else {
    return bytes; 
  }
  
};