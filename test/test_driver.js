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


var assert = require('assert');
var console = require('console');
var EventEmitter = require('events').EventEmitter;

var BigInteger = require('../lib/bigint').BigInteger;

var Connection = require('../lib/driver').Connection;
var PooledConnection = require('../lib/driver').PooledConnection;
var ttypes = require('../lib/gen-nodejs/cassandra_types');
var Keyspace = require('../node-cassandra-client').Keyspace;
var System = require('../lib/system').System;
var KsDef = require('../lib/system').KsDef;
var CfDef = require('../lib/system').CfDef;

var CASSANDRA_PORT = 9170;

function stringToHex(s) {
  var buf = '';
  for (var i = 0; i < s.length; i++) {
    buf += s.charCodeAt(i).toString(16);
  }
  return buf;
}


function connect(callback) {
  var handler = new EventEmitter();
  handler.on('error', function(err) {
    callback(err, null);
  });
  handler.on('ready', function(con) {
    callback(null, con);
  });
  var con = new Connection('127.0.0.1', CASSANDRA_PORT, 'Keyspace1');
  con.connect(function(err) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, con);
    }
  });
}

exports.setUp = function(test, assert) {
  var sys = new System('127.0.0.1:'+CASSANDRA_PORT);
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
          test.finish();
        }
      });
    } else {
      close();
      console.log(ksDef.name + ' keyspace already exists');
      test.finish();
    }
  });
};

exports.testWhiskyIsWorking = function(test, assert) {
  assert.throws(function() {
      assert.ok(false);
  }, require('assert').AssertionError);
  test.finish();
}

exports.testSimpleUpdate = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ifError(err);
      test.finish();
    } else {
      var key = stringToHex('key0');
      con.execute('update Standard1 set ?=?, ?=? where key=?', ['cola', 'valuea', 'colb', 'valueb', key], function(updateErr) {
        if (updateErr) {
          con.close();
          assert.ifError(updateErr);
          test.finish();
        } else {
          con.execute('select ?, ? from Standard1 where key=?', ['cola', 'colb', key], function(selectErr, rows) {
            con.close();
            assert.ifError(selectErr);
            assert.strictEqual(rows.rowCount(), 1);
            var row = rows[0];
            assert.strictEqual('cola', row.cols[0].name);
            assert.strictEqual('valuea', row.cols[0].value);
            test.finish();
          });
        }
      });
    }
  });
};

exports.testUpdateWithNull = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ifError(err);
      test.finish();
    } else {
      var key = stringToHex('key0');
      con.execute('update Standard1 set ?=?, ?=? where key=?', ['cola', null, 'colb', 'valueb', key], function(updateErr) {
        con.close();
        assert.ok(updateErr);
        test.finish();
      });
    }
  });
};

exports.testSimpleDelete = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      var key = stringToHex('key2');
      con.execute('update Standard1 set ?=?, ?=? where key=?', ['colx', 'xxx', 'colz', 'bbb', key], function(updateErr) {
        if (updateErr) {
          con.close();
          
        } else {
          con.execute('delete ?,? from Standard1 where key in (?)', ['colx', 'colz', key], function(delErr) {
            if (delErr) {
              con.close();
              assert.ok(false);
              test.finish();
            } else {
              con.execute('select ?,? from Standard1 where key=?', ['colx', 'colz', key], function(selErr, rows) {
                con.close();
                if (selErr) {
                  assert.ok(false);
                } else {
                  assert.strictEqual(rows.rowCount(), 1);
                  var row = rows[0];
                  assert.strictEqual(0, row.colCount());
                }
                test.finish();
              });
            }
          });
        }
      });
    }
  });
};

exports.testLong = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      // the third pair is ±2^62, which overflows the 53 bits in the fp mantissa js uses for numbers (should lose precision
      // coming back), but still fits nicely in an 8-byte long (it should work).
      // notice how updParams will take either a string or BigInteger
      var key = 123456;
      var updParms = [1, 2, 3, 4, '4611686018427387904', new BigInteger('-4611686018427387904'), key];
      var selParms = [1, 3, new BigInteger('4611686018427387904'), key];
      con.execute('update CfLong set ?=?,?=?,?=? where key=?', updParms, function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select ?,?,? from CfLong where key=?', selParms, function(selErr, rows) {
            con.close();
            if (selErr) {
              assert.ok(false);
            } else {
              assert.strictEqual(rows.rowCount(), 1);
              var row = rows[0];
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
            }
            test.finish();
          });
        }
      });
    }
  });
};

exports.testSlice = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      con.execute('update CfLong set -5=-55, -4=-44, -3=-33, -2=-22, -1=-11, 0=0, 1=11, 2=22, 3=33, 4=44, 5=55 where key=12345', [], function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select ?..? from CfLong where key=12345', [-2, 2], function(selErr, rows) {
            con.close();
            if (selErr) {
              assert.ok(false);
            } else {
              assert.strictEqual(rows.rowCount(), 1);
              var row = rows[0];
              assert.strictEqual(5, row.colCount());
              assert.ok(row.cols[1].name.equals(new BigInteger('-1')));
              assert.ok(row.cols[1].value.equals(new BigInteger('-11')));
              assert.ok(row.cols[3].name.equals(new BigInteger('1')));
              assert.ok(row.cols[3].value.equals(new BigInteger('11')));
            }
            test.finish();
          });
        }
      });
    }
  }); 
};

exports.testReverseSlice = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      con.execute('update CfLong set -5=-55, -4=-44, -3=-33, -2=-22, -1=-11, 0=0, 1=11, 2=22, 3=33, 4=44, 5=55 where key=12345', [], function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select REVERSED ?..? from CfLong where key=12345', [2, -2], function(selErr, rows) {
            con.close();
            if (selErr) {
              assert.ok(false);
            } else {
              assert.strictEqual(rows.rowCount(), 1);
              var row = rows[0];
              assert.strictEqual(5, row.colCount());
              assert.ok(row.cols[3].name.equals(new BigInteger('-1')));
              assert.ok(row.cols[3].value.equals(new BigInteger('-11')));
              assert.ok(row.cols[1].name.equals(new BigInteger('1')));
              assert.ok(row.cols[1].value.equals(new BigInteger('11')));
            }
            test.finish();
          });
        }
      });
    }
  });
};

exports.testReversedSliceLimit = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      con.execute('update CfLong set -5=-55, -4=-44, -3=-33, -2=-22, -1=-11, 0=0, 1=11, 2=22, 3=33, 4=44, 5=55 where key=12345', [], function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select first 3 REVERSED ?..? from CfLong where key=12345', [2, -2], function(selErr, rows) {
            con.close();
            if (selErr) {
              assert.ok(false);              
            } else {
              assert.strictEqual(rows.rowCount(), 1);
              var row = rows[0]
              assert.strictEqual(3, row.colCount());
              assert.ok(row.cols[1].name.equals(new BigInteger('1')));
              assert.ok(row.cols[1].value.equals(new BigInteger('11')));
              assert.ok(row.cols[2].name.equals(new BigInteger('0')));
              assert.ok(row.cols[2].value.equals(new BigInteger('0')));
              assert.equal(row.cols[2].name, 0);
              assert.equal(row.cols[2].value, 0);
            }
            test.finish();
          });
        }
      });
    }
  });
};

exports.testReversedSlice = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      con.execute('update CfLong set -5=-55, -4=-44, -3=-33, -2=-22, -1=-11, 0=0, 1=11, 2=22, 3=33, 4=44, 5=55 where key=12345', [], function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select REVERSED ?..? from CfLong where key=12345', [2, -2], function(selErr, rows) {
            con.close();
            if (selErr) {
              assert.ok(false);
            } else {
              assert.strictEqual(rows.rowCount(), 1);
              var row = rows[0];
              assert.strictEqual(5, row.colCount());
              assert.ok(row.cols[3].name.equals(new BigInteger('-1')));
              assert.ok(row.cols[3].value.equals(new BigInteger('-11')));
              assert.ok(row.cols[1].name.equals(new BigInteger('1')));
              assert.ok(row.cols[1].value.equals(new BigInteger('11')));
            }
            test.finish();
          });
        }
      });
    }
  });
};

exports.testInt = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      // make sure to use some numbers that will overflow a 64 bit signed value.
      var updParms = [1, 11, -1, -11, '8776496549718567867543025521', '-8776496549718567867543025521', '3456543434345654345332453455633'];
      var selParms = [-1, 1, '8776496549718567867543025521', '3456543434345654345332453455633'];
      con.execute('update CfInt set ?=?, ?=?, ?=? where key=?', updParms, function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select ?, ?, ? from CfInt where key=?', selParms, function(selErr, rows) {
            con.close();
            if (selErr) {
              assert.ok(false);
            } else {
              var row = rows[0];
              assert.strictEqual(rows.rowCount(), 1);
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
            }
            test.finish();
          });
        }
      });
    }
  });
};

exports.testUUID = function(test, assert) {
  // make sure we're not comparing the same things.
  assert.ok(!new UUID('string', '6f8483b0-65e0-11e0-0000-fe8ebeead9fe').equals(new UUID('string', '6fd589e0-65e0-11e0-0000-7fd66bb03aff')));
  assert.ok(!new UUID('string', '6fd589e0-65e0-11e0-0000-7fd66bb03aff').equals(new UUID('string', 'fa6a8870-65fa-11e0-0000-fe8ebeead9fd')));
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      // again, demonstrate that we can use strings or objectifications.
      var updParms = ['6f8483b0-65e0-11e0-0000-fe8ebeead9fe', '6fd45160-65e0-11e0-0000-fe8ebeead9fe', '6fd589e0-65e0-11e0-0000-7fd66bb03aff', '6fd6e970-65e0-11e0-0000-fe8ebeead9fe', 'fa6a8870-65fa-11e0-0000-fe8ebeead9fd'];
      var selParms = ['6f8483b0-65e0-11e0-0000-fe8ebeead9fe', '6fd589e0-65e0-11e0-0000-7fd66bb03aff', 'fa6a8870-65fa-11e0-0000-fe8ebeead9fd'];
      con.execute('update CfUuid set ?=?, ?=? where key=?', updParms, function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select ?, ? from CfUuid where key=?', selParms, function(selErr, rows) {
            con.close();
            if (selErr) { 
              assert.ok(false);
            } else {
              assert.strictEqual(rows.rowCount(), 1);
              var row = rows[0];
              assert.strictEqual(2, row.colCount());
              
              assert.ok(new UUID('string', '6f8483b0-65e0-11e0-0000-fe8ebeead9fe').equals(row.cols[0].name));
              assert.ok(new UUID('string', '6fd45160-65e0-11e0-0000-fe8ebeead9fe').equals(row.cols[0].value));
              assert.ok(new UUID('string', '6fd589e0-65e0-11e0-0000-7fd66bb03aff').equals(row.cols[1].name));
              assert.ok(new UUID('string', '6fd6e970-65e0-11e0-0000-fe8ebeead9fe').equals(row.cols[1].value));
              
              assert.ok(row.colHash[(new UUID('string', '6fd589e0-65e0-11e0-0000-7fd66bb03aff'))].equals(row.cols[1].value));
              assert.ok(row.colHash[(row.cols[0].name)].equals(row.cols[0].value));
              assert.ok(row.colHash[(new UUID('string', '6f8483b0-65e0-11e0-0000-fe8ebeead9fe'))].equals(row.cols[0].value));
              assert.ok(row.colHash[(row.cols[1].name)].equals(row.cols[1].value));
            }
            test.finish();
          });
        }
      });
    }
  });
};

exports.testCustomValidators = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      var updParms = ['normal', 25, 'int_col', 21, 'string_col', 'test_string_value', 'uuid_col', '6f8483b0-65e0-11e0-0000-fe8ebeead9fe', 211];
      var selParms = ['normal', 'int_col', 'string_col', 'uuid_col', 211];
      con.execute('update CfUgly set ?=?, ?=?, ?=?, ?=? where key=?', updParms, function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select  ?, ?, ?, ? from CfUgly where key=?', selParms, function(selErr, rows) {
            con.close();
            if (selErr) {
              assert.ok(false);
            } else {
              assert.strictEqual(rows.rowCount(), 1);
              var row = rows[0];
              assert.strictEqual(4, row.colCount());
              
              assert.ok(row.colHash.normal.equals(new BigInteger('25')));
              assert.ok(row.colHash.int_col.equals(new BigInteger('21')));
              assert.ok(row.colHash.string_col === 'test_string_value');
              assert.ok(row.colHash.uuid_col.toString() == '6f8483b0-65e0-11e0-0000-fe8ebeead9fe');
            }
            test.finish();
          });
        }
      });
    }
  });
};

// this test only works an order-preserving partitioner.
// it also uses an event-based approach to doing things.
//exports.DISABLED_testMultipleRows = function(test, assert) {
//  // go through the motions of creating a new keyspace every time. we do this to ensure only the things in there are 
//  // what I expect.
//  
//  var sys = new Connection('127.0.0.1', CASSANDRA_PORT, 'system');
//  sys.connect(function(err) {
//    if (err) {
//      assert.ok(false)
//      test.finish();
//    } else {
//      var ev = new EventEmitter();
//      // attempt to drop the keyspace on error.
//      ev.on('syserr', function() {
//        console.log('syserr');
//        sys.execute('drop keyspace ints', function(err) {});
//        sys.close();
//        assert.ok(false);
//        test.finish();
//      });
//    
//      // keyspace is there for sure. don't know about the cf.
//      ev.on('ksready', function() {
//        console.log('keyspace created');
//        sys.close();
//        var con = new Connection('127.0.0.1', CASSANDRA_PORT, 'ints');
//        con.execute('create columnfamily cfints (key int primary key) with comparator=int and default_validation=int', null, function(err) {
//          con.close();
//          if (err) {
//            ev.emit('syserr');
//          } else {
//            ev.emit('cfready');
//          }
//        });
//        con.close();
//      });
//      
//      // column family is ready, do the test.
//      ev.on('cfready', function() {
//        
//        // insert 100 rows.
//        var con = new Connection('127.0.0.1', 9160, 'ints');
//        var count = 100;
//        var num = 0;
//        for (var i = 0; i < count; i++) {
//          con.execute('update cfints set ?=? where key=?', [1, i, i], function(err) {
//            if (err) {
//              con.close();
//              ev.emit('syserr');
//            } else {
//              num += 1;
//              
//              // after all the rows are in, do a query.
//              if (num >= count) {
//                con.execute('select ? from cfints where key > ? and key < ?', [1, 10, 20], function(serr, rows) {
//                  con.close();
//                  assert.strictEqual(rows.rowCount(), 11);
//                });
//              }
//            }
//          });
//        }
//      });
//      
//      // start everything off.
//      sys.execute('drop keyspace ints', null, function(dropErr) {
//        if (!dropErr) {
//          console.log('keyspace dropped');
//        }
//        sys.execute('create keyspace ints with strategy_class=SimpleStrategy and strategy_options:replication_factor=1', null, function(createKsErr) {
//          if (createKsErr) {
//            ev.emit('syserr');
//          } else {
//            ev.emit('ksready');
//          }
//        });
//      });
//    }
//  });
//};

exports.testPooledConnection = function(test, assert) {
  function bail(conn, err) {
    conn.shutdown();
    assert.ifError(err);
    test.finish();
  }
  
  //var hosts = ["127.0.0.2:9170", "127.0.0.1:9170"];
  var hosts = ["127.0.0.1:9170"];
  var conn = new PooledConnection({'hosts': hosts, 'keyspace': 'Keyspace1'});
  
  // Hammer time...
  conn.execute('UPDATE CfUgly SET A=1 WHERE KEY=1', [], function(err) {
    if (err) { bail(conn, err); }
    
    for (var i = 0; i < 100; i++) {
      conn.execute('SELECT A FROM CfUgly WHERE KEY=1', [], function(err, rows) {
        if (err) { bail(conn, err); }
        assert.strictEqual(rows.rowCount(), 1);
        var row = rows[0];
        assert.strictEqual(row.cols[0].name, 'A');
      });
    }
    
    conn.shutdown();
    test.finish();
  });
};


