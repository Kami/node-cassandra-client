var assert = require('assert');
var console = require('console');
var ColumnFamily = require('../node-cassandra-client').ColumnFamily;
var System = require('../node-cassandra-client').System;
var KsDef = require('../node-cassandra-client').KsDef;
var CfDef = require('../node-cassandra-client').CfDef;
var Pool = require('../lib/pool').Pool;

/**
 * This test assumes you have cassandra running on 127.0.0.1:9160 (of course, other hosts are allowed too).
 */


/**
 * creates the test keyspace if we need it.
 * callback is called if the keyspace already exists or is created successfully.
 */
function maybeCreateKeyspace(callback) {
  var sys = new System('127.0.0.1:9160');
  var ksName = 'Keyspace1';
  var close = function() {
    sys.close(function() {
      sys.close();
      console.log('System keyspace closed');
    });
  };
  sys.describeKeyspace(ksName, function(err, ksDef) {
    if (err) {
      var standard1 = new CfDef({keyspace: ksName, name: 'Standard1', column_type: 'Standard', comparator_type: 'UTF8Type'});
      var super1 = new CfDef({keyspace: ksName, name: 'Super1', column_type: 'Super', comparator_type: 'UTF8Type', subcomparator_type: 'UTF8Type'});
      var keyspace1 = new KsDef({name: ksName, strategy_class: 'org.apache.cassandra.locator.SimpleStrategy', replication_factor:1, cf_defs: [standard1, super1]});
      sys.addKeyspace(keyspace1, function(err) {
        close();
        if (err) {
          assert.ifError(err);
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

/** ensures the test keyspace is created */
exports['setUp'] = function(callback) {
  maybeCreateKeyspace(callback);
};

/** simple inserts. */
exports['testLongInsert'] = function() {
  var CfLong = new ColumnFamily('Keyspace1', 'CfLong', null, null, '127.0.0.1', 9160);
  CfLong.insert(1, {11:22, 33:44, 55:66}, 0, 'ONE', function(err) {
    CfLong.close();
    if (err) {
      console.log(err);
      throw new Error(err);
    }
  });
}

exports['testUUIDInsert'] = function() {
  var CfUUID = new ColumnFamily('Keyspace1', 'CfUuid', null, null, '127.0.0.1', 9160);
  CfUUID.insert('6f8483b0-65e0-11e0-0000-fe8ebeead9fe',
                {
                  '6f8483b0-65e0-11e0-0000-fe8ebeead9fe': '6fd45160-65e0-11e0-0000-fe8ebeead9fe',
                  '6fd589e0-65e0-11e0-0000-7fd66bb03aff': '6fd6e970-65e0-11e0-0000-fe8ebeead9fe'
                },
                0, 'ONE', function(err) {
                            CfUUID.close();
                            if (err) {
                              console.log(err);
                              throw new Error(err);
                            }
                          });
};

//exports.setUp(function() {
// exports.testSimpleInsert();
//});