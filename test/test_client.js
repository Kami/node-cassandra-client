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

function insert(cf, key, cols, callback) {
  cf.insert(key, cols, 0, 'ONE', callback);
}

function cfLong() {
  return new ColumnFamily('Keyspace1', 'CfLong', null, null, '127.0.0.1', 9160);
}

function cfUuid() {
  return new ColumnFamily('Keyspace1', 'CfUuid', null, null, '127.0.0.1', 9160);
}

/** simple inserts. */
exports['testLongInsert'] = function() {
  var CfLong = cfLong();
  insert(CfLong, 1, {11:22, 33:44, 55:66}, function(err) {
    CfLong.close();
    if (err) {
      console.log(err);
      throw new Error(err);
    }
  });
};

exports['testLongDelete'] = function() {
  var CfLong = cfLong();
  var key = 2;
  insert(CfLong, key, {1:2, 3:4, 5:6, 7:8}, function (updErr) {
    if (updErr) {
      CfLong.close();
      throw new Error(updErr);
    } else {
      CfLong.remove(key, [1, 3, 5, 7], 'ONE', function(delErr) {
        CfLong.close();
        if (delErr) {
          throw new Error(delErr);
        }
      });
    }
  });
};

exports['testLongGetSlice'] = function() {
  var CfLong = cfLong();
  var key = 3;
  insert(CfLong, key, {1:2, 3:4, 5:6, 7:8}, function (updErr) {
    if (updErr) {
      CfLong.close();
      throw new Error(updErr);
    } else {
      var COL_LIMIT = 3;
      CfLong.get(key, {start:1, finish:100}, false, COL_LIMIT, 'ONE', function(selectErr, cols) {
        CfLong.close();
        if (selectErr) {
          console.log(selectErr);
          throw new Error(selectErr);
        } else {
          assert.strictEqual(cols.size(), COL_LIMIT);
          assert.ok(cols[1].equals(new BigInteger('2')));
          assert.ok(cols[3].equals(new BigInteger('4')));
          assert.ok(cols[5].equals(new BigInteger('6')));
        }
      });
    }
  });
};

exports['testLongGetCols'] = function() {
  var CfLong = cfLong();
  var key = 4;
  insert(CfLong, key, {1:2, 3:4, 5:6, 7:8}, function (updErr) {
    if (updErr) {
      CfLong.close();
      throw new Error(updErr);
    } else {
      var COL_LIMIT = 3;
      CfLong.get(key, [1,5,7], false, COL_LIMIT, 'ONE', function(selectErr, cols) {
        CfLong.close();
        if (selectErr) {
          console.log(selectErr);
          throw new Error(selectErr);
        } else {
          assert.strictEqual(cols.size(), COL_LIMIT);
          assert.ok(cols[1].equals(new BigInteger('2')));
          assert.ok(cols[5].equals(new BigInteger('6')));
          assert.ok(cols[7].equals(new BigInteger('8')));
        }
      });
    }
  });
};

exports['testUUIDInsert'] = function() {
  var CfUUID = cfUuid();
  var cols = {'6f8483b0-65e0-11e0-0000-fe8ebeead9fe': '6fd45160-65e0-11e0-0000-fe8ebeead9fe',
              '6fd589e0-65e0-11e0-0000-7fd66bb03aff': '6fd6e970-65e0-11e0-0000-fe8ebeead9fe'};
  insert(CfUUID, '6f8483b0-65e0-11e0-0000-fe8ebeead9fe', cols, function(err) {
    CfUUID.close();
    if (err) {
      console.log(err);
      throw new Error(err);
    }
  });
};

//exports.setUp(function() {
//  exports.testLongGetCols();
//});