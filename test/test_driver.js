
var assert = require('assert');
var console = require('console');
var EventEmitter = require('events').EventEmitter;

var BigInteger = require('../lib/bigint').BigInteger;

var Connection = require('../lib/driver').Connection;
var ttypes = require('../lib/gen-nodejs/cassandra_types');
var Keyspace = require('../node-cassandra-client').Keyspace;
var System = require('../lib/system').System;
var KsDef = require('../lib/system').KsDef;
var CfDef = require('../lib/system').CfDef;

function stringToHex(s) {
  var buf = '';
  for (var i = 0; i < s.length; i++) {
    buf += s.charCodeAt(i).toString(16);
  }
  return buf;
}

function maybeCreateKeyspace(callback) {
  var sys = new System('127.0.0.1:9160');
  var ksName = 'Keyspace1';
  var close = function() {
    sys.close(function() {
      sys.close();
      console.log('System keyspace closed');
    });
  };
  sys.describeKeyspace(ksName, function(descErr, ksDef) {
    if (descErr) {
      console.log('adding test keyspace');
      var standard1 = new CfDef({keyspace: ksName, name: 'Standard1', column_type: 'Standard', comparator_type: 'UTF8Type', default_validation_class: 'UTF8Type'});
      var cfLong = new CfDef({keyspace: ksName, name: 'CfLong', column_type: 'Standard', comparator_type: 'LongType', default_validation_class: 'LongType', key_validation_class: 'LongType'});
      var cfInt = new CfDef({keyspace: ksName, name: 'CfInt', column_type: 'Standard', comparator_type: 'IntegerType', default_validation_class: 'IntegerType', key_validation_class: 'IntegerType'});
      var cfUtf8 = new CfDef({keyspace: ksName, name: 'CfUtf8', column_type: 'Standard', comparator_type: 'UTF8Type', default_validation_class: 'UTF8Type', key_validation_class: 'UTF8Type'});
      var cfBytes = new CfDef({keyspace: ksName, name: 'CfBytes', column_type: 'Standard', comparator_type: 'BytesType', default_validation_class: 'BytesType', key_validation_class: 'BytesType'});
      var cfUuid = new CfDef({keyspace: ksName, name: 'CfUuid', column_type: 'Standard', comparator_type: 'TimeUUIDType', default_validation_class: 'TimeUUIDType', key_validation_class: 'TimeUUIDType'});
      var cfUgly = new CfDef({keyspace: ksName, name: 'CfUgly', column_type: 'Standard', comparator_type: 'UTF8Type', 
                              default_validation_class: 'LongType', key_validation_class: 'IntegerType', 
                              column_metadata: [
                                new ttypes.ColumnDef({name: 'int_col', validation_class: 'IntegerType'}),
                                new ttypes.ColumnDef({name: 'string_col', validation_class: 'UTF8Type'}),
                                new ttypes.ColumnDef({name: 'uuid_col', validation_class: 'TimeUUIDType'})
                              ]});
      var super1 = new CfDef({keyspace: ksName, name: 'Super1', column_type: 'Super', comparator_type: 'UTF8Type', subcomparator_type: 'UTF8Type'});
      var keyspace1 = new KsDef({name: ksName, strategy_class: 'org.apache.cassandra.locator.SimpleStrategy', strategy_options: {'replication_factor': '1'}, cf_defs: [standard1, super1, cfInt, cfUtf8, cfLong, cfBytes, cfUuid, cfUgly]});
      sys.addKeyspace(keyspace1, function(addErr) {
        console.log(addErr);
        close();
        if (addErr) {
          assert.ifError(addErr);
        } else {
          console.log('keyspace created');
          callback();
        }
      });
    } else {
      close();
      console.log(ksDef.name + ' keyspace already exists');
      callback();
    }
  });
}

function connect() {
  return new Connection(null, null, '127.0.0.1', 9160, 'Keyspace1');
}

exports['setUp'] = function(callback) {
  maybeCreateKeyspace(callback);
};

exports['testSimpleUpdate'] = function() {
  var con = connect();
  var key = stringToHex('key0');
  con.execute('update Standard1 set ?=?, ?=? where key=?', ['cola', 'valuea', 'colb', 'valueb', key], function(updateErr) {
    if (updateErr) {
      con.close();
      throw new Error(updateErr);
    } else {
      con.execute('select ?, ? from Standard1 where key=?', ['cola', 'colb', key], function(selectErr, row) {
        con.close();
        if (selectErr) {
          console.log(selectErr);
          throw new Error(selectErr);
        }
        assert.strictEqual('cola', row.cols[0].name);
        assert.strictEqual('valuea', row.cols[0].value);
      });
    }
  });
};

exports['testSimpleDelete'] = function() {
  var con = connect();
  var key = stringToHex('key1');
  con.execute('update Standard1 set ?=?, ?=? where key=?', ['colx', 'xxx', 'colz', 'bbb', key], function(updateErr) {
    if (updateErr) {
      con.close();
      throw new Error(updateErr);
    } else {
      con.execute('delete ?,? from Standard1 where key=?', ['colx', 'colz', key], function(delErr) {
        if (delErr) {
          con.close();
          throw new Error(delErr);
        } else {
          con.execute('select ?,? from Standard1 where key=?', ['colx', 'colz', key], function(selErr, row) {
            con.close();
            if (selErr) {
              throw new Error(selErr);
            } else {
              assert.strictEqual(0, row.colCount());
            }
          });
        }
      });
    }
  });
};

exports['testLong'] = function() {
  var con = connect();
  // the third pair is ±2^62, which overflows the 53 bits in the fp mantissa js uses for numbers (should lose precision
  // coming back), but still fits nicely in an 8-byte long (it should work).
  // notice how updParams will take either a string or BigInteger
  var key = 123456;
  var updParms = [1, 2, 3, 4, '4611686018427387904', new BigInteger('-4611686018427387904'), key];
  var selParms = [1, 3, new BigInteger('4611686018427387904'), key];
  con.execute('update CfLong set ?=?,?=?,?=? where key=?', updParms, function(updErr) {
    if (updErr) {
      con.close();
      throw new Error(updErr);
    } else {
      con.execute('select ?,?,? from CfLong where key=?', selParms, function(selErr, row) {
        con.close();
        if (selErr) {
          throw new Error(selErr);
        }
        assert.strictEqual(3, row.colCount());
        
        assert.ok(new BigInteger('1').equals(row.cols[0].name));
        assert.ok(new BigInteger('2').equals(row.cols[0].value));
        assert.ok(new BigInteger('3').equals(row.cols[1].name));
        assert.ok(new BigInteger('4').equals(row.cols[1].value));
        assert.ok(new BigInteger('4611686018427387904').equals(row.cols[2].name));
        assert.ok(new BigInteger('-4611686018427387904').equals(row.cols[2].value));
        
        assert.ok(new BigInteger('2').equals(row.colHash['1']));
        assert.ok(new BigInteger('4').equals(row.colHash['3']));
        assert.ok(new BigInteger('-4611686018427387904').equals(row.colHash['4611686018427387904']));
      });
    }
  });
};

exports['testSlice'] = function() {
  var con = connect();
  con.execute('update CfLong set -5=-55, -4=-44, -3=-33, -2=-22, -1=-11, 0=0, 1=11, 2=22, 3=33, 4=44, 5=55 where key=12345', [], function(updErr) {
    if (updErr) {
      con.close();
      throw new Error(updErr);
    } else {
      con.execute('select ?..? from CfLong where key=12345', [-2, 2], function(selErr, row) {
        con.close();
        if (selErr) {
          throw new Error(selErr);
        } else {
          assert.strictEqual(5, row.colCount());
          assert.ok(row.cols[1].name.equals(new BigInteger('-1')));
          assert.ok(row.cols[1].value.equals(new BigInteger('-11')));
          assert.ok(row.cols[3].name.equals(new BigInteger('1')));
          assert.ok(row.cols[3].value.equals(new BigInteger('11')));
        }
      });
    }
  });
};

exports['testReverseSlice'] = function() {
  var con = connect();
  con.execute('update CfLong set -5=-55, -4=-44, -3=-33, -2=-22, -1=-11, 0=0, 1=11, 2=22, 3=33, 4=44, 5=55 where key=12345', [], function(updErr) {
    if (updErr) {
      con.close();
      throw new Error(updErr);
    } else {
      con.execute('select REVERSED ?..? from CfLong where key=12345', [2, -2], function(selErr, row) {
        con.close();
        if (selErr) {
          throw new Error(selErr);
        } else {
          assert.strictEqual(5, row.colCount());
          assert.ok(row.cols[3].name.equals(new BigInteger('-1')));
          assert.ok(row.cols[3].value.equals(new BigInteger('-11')));
          assert.ok(row.cols[1].name.equals(new BigInteger('1')));
          assert.ok(row.cols[1].value.equals(new BigInteger('11')));
        }
      });
    }
  });
};

exports['testReversedSliceLimit'] = function() {
  var con = connect();
  con.execute('update CfLong set -5=-55, -4=-44, -3=-33, -2=-22, -1=-11, 0=0, 1=11, 2=22, 3=33, 4=44, 5=55 where key=12345', [], function(updErr) {
    if (updErr) {
      con.close();
      throw new Error(updErr);
    } else {
      con.execute('select first 3 REVERSED ?..? from CfLong where key=12345', [2, -2], function(selErr, row) {
        con.close();
        if (selErr) {
          throw new Error(selErr);
        } else {
          assert.strictEqual(3, row.colCount());
          assert.ok(row.cols[1].name.equals(new BigInteger('1')));
          assert.ok(row.cols[1].value.equals(new BigInteger('11')));
          assert.ok(row.cols[2].name.equals(new BigInteger('0')));
          assert.ok(row.cols[2].value.equals(new BigInteger('0')));
          assert.equal(row.cols[2].name, 0);
          assert.equal(row.cols[2].value, 0);
        }
      });
    }
  });
};

exports['testReversedSlice'] = function() {
  var con = connect();
  con.execute('update CfLong set -5=-55, -4=-44, -3=-33, -2=-22, -1=-11, 0=0, 1=11, 2=22, 3=33, 4=44, 5=55 where key=12345', [], function(updErr) {
    if (updErr) {
      con.close();
      throw new Error(updErr);
    } else {
      con.execute('select REVERSED ?..? from CfLong where key=12345', [2, -2], function(selErr, row) {
        con.close();
        if (selErr) {
          throw new Error(selErr);
        } else {
          assert.strictEqual(5, row.colCount());
          assert.ok(row.cols[3].name.equals(new BigInteger('-1')));
          assert.ok(row.cols[3].value.equals(new BigInteger('-11')));
          assert.ok(row.cols[1].name.equals(new BigInteger('1')));
          assert.ok(row.cols[1].value.equals(new BigInteger('11')));
        }
      });
    }
  });
}

exports['testInt'] = function() {
  var con = connect();
  // make sure to use some numbers that will overflow a 64 bit signed value.
  var updParms = [1, 11, -1, -11, '8776496549718567867543025521', '-8776496549718567867543025521', '3456543434345654345332453455633'];
  var selParms = [-1, 1, '8776496549718567867543025521', '3456543434345654345332453455633'];
  con.execute('update CfInt set ?=?, ?=?, ?=? where key=?', updParms, function(updErr) {
    if (updErr) {
      con.close();
      throw new Error(updErr);
    } else {
      con.execute('select ?, ?, ? from CfInt where key=?', selParms, function(selErr, row) {
        con.close();
        if (selErr) {
          throw new Error(selErr);
        }
        assert.strictEqual(3, row.colCount());
        
        assert.ok(new BigInteger('-1').equals(row.cols[0].name));
        assert.ok(new BigInteger('-11').equals(row.cols[0].value));
        assert.ok(new BigInteger('1').equals(row.cols[1].name));
        assert.ok(new BigInteger('11').equals(row.cols[1].value));
        assert.ok(new BigInteger('8776496549718567867543025521').equals(row.cols[2].name));
        assert.ok(new BigInteger('-8776496549718567867543025521').equals(row.cols[2].value));
        
        assert.ok(new BigInteger('11').equals(row.colHash['1']));
        assert.ok(new BigInteger('-11').equals(row.colHash['-1']));
        assert.ok(new BigInteger('-8776496549718567867543025521').equals(row.colHash['8776496549718567867543025521']));
      });
    }
  });
};

exports['testUUID'] = function() {
  // make sure we're not comparing the same things.
  assert.ok(!new UUID('string', '6f8483b0-65e0-11e0-0000-fe8ebeead9fe').equals(new UUID('string', '6fd589e0-65e0-11e0-0000-7fd66bb03aff')));
  assert.ok(!new UUID('string', '6fd589e0-65e0-11e0-0000-7fd66bb03aff').equals(new UUID('string', 'fa6a8870-65fa-11e0-0000-fe8ebeead9fd')));
  var con = connect();
  // again, demonstrate that we can use strings or objectifications.
  var updParms = ['6f8483b0-65e0-11e0-0000-fe8ebeead9fe', '6fd45160-65e0-11e0-0000-fe8ebeead9fe', '6fd589e0-65e0-11e0-0000-7fd66bb03aff', '6fd6e970-65e0-11e0-0000-fe8ebeead9fe', 'fa6a8870-65fa-11e0-0000-fe8ebeead9fd'];
  var selParms = ['6f8483b0-65e0-11e0-0000-fe8ebeead9fe', '6fd589e0-65e0-11e0-0000-7fd66bb03aff', 'fa6a8870-65fa-11e0-0000-fe8ebeead9fd'];
  con.execute('update CfUuid set ?=?, ?=? where key=?', updParms, function(updErr) {
    if (updErr) {
      con.close();
      throw new Error(updErr);
    } else {
      con.execute('select ?, ? from CfUuid where key=?', selParms, function(selErr, row) {
        con.close();
        if (selErr) { 
          throw new Error(selErr);
        }
        assert.strictEqual(2, row.colCount());
        
        assert.ok(new UUID('string', '6f8483b0-65e0-11e0-0000-fe8ebeead9fe').equals(row.cols[0].name));
        assert.ok(new UUID('string', '6fd45160-65e0-11e0-0000-fe8ebeead9fe').equals(row.cols[0].value));
        assert.ok(new UUID('string', '6fd589e0-65e0-11e0-0000-7fd66bb03aff').equals(row.cols[1].name));
        assert.ok(new UUID('string', '6fd6e970-65e0-11e0-0000-fe8ebeead9fe').equals(row.cols[1].value));
        
        assert.ok(row.colHash[(new UUID('string', '6fd589e0-65e0-11e0-0000-7fd66bb03aff'))].equals(row.cols[1].value));
        assert.ok(row.colHash[(row.cols[0].name)].equals(row.cols[0].value));
        assert.ok(row.colHash[(new UUID('string', '6f8483b0-65e0-11e0-0000-fe8ebeead9fe'))].equals(row.cols[0].value));
        assert.ok(row.colHash[(row.cols[1].name)].equals(row.cols[1].value));
      });
    }
  });
};

exports['testCustomValidators'] = function() {
  var con = connect();
  var updParms = ['normal', 25, 'int_col', 21, 'string_col', 'test_string_value', 'uuid_col', '6f8483b0-65e0-11e0-0000-fe8ebeead9fe', 211];
  var selParms = ['normal', 'int_col', 'string_col', 'uuid_col', 211];
  con.execute('update CfUgly set ?=?, ?=?, ?=?, ?=? where key=?', updParms, function(updErr) {
    if (updErr) {
      con.close();
      throw new Error(updErr);
    } else {
      con.execute('select  ?, ?, ?, ? from CfUgly where key=?', selParms, function(selErr, row) {
        con.close();
        if (selErr) {
          throw new Error(selErr);
        }
        assert.strictEqual(4, row.colCount());
        
        assert.ok(row.colHash['normal'].equals(new BigInteger('25')));
        assert.ok(row.colHash['int_col'].equals(new BigInteger('21')));
        assert.ok(row.colHash['string_col'] === 'test_string_value');
        assert.ok(row.colHash['uuid_col'].toString() == '6f8483b0-65e0-11e0-0000-fe8ebeead9fe');
      });
    }
  });
};

// rename to 'testMultipleRows' to get whiskey to call it.
exports[' MultipleRows'] = function() {
  var con = connect();
  var count = 100;
  var num = 0;
  for (var i = 0; i < count; i++) {
//    con.execute('update CfLong set ?=?, ?=?, ?=? where key=?', [1, 1000 * i, 2, 1000000 * i, 3, 100000000 * i, 1000000 + i], function(err) {
    con.execute('update CfUtf8 set ?=? where key=?', ['cola', 'value' + i, 'abcdefghijklmnopqrstuvwxyz'+i], function(err) {
      if (err) {
        throw new Error(err);
      } else {
        num += 1;
        if (num >= count) {
          // do the selection
//          con.execute('select ?, ?, ? from CfLong where key >= ? and key <= ?', [1, 2, 3, 1000000, 1000099], function(err, rows) {
          con.execute('select ? from CfUtf8 where key <= ? and key >= ?', ['cola', 'abcdefghijklmnopqrstuvwxyz', 'abcdefghijklmnopqrstuvwxyz9'], function(err, rows) {
            con.close();
            if (err) {
              console.log(err);
              throw new Error(err);
            } else {
              assert.ok(rows.rowCount() > 0);
              console.log(rows.rowCount());
              console.log(rows);
            }
          });
        }
      }
    });
  }
};


//this is for running some of the tests outside of whiskey.
//maybeCreateKeyspace(function() {
//  exports.testLong();
//  exports.testInt()
//  exports.testUUID();
//  exports.testSlice();
//  exports.testReverseSlice();
//  exports.testSliceLimit();
//  exports.testMultipleRows();
//});
