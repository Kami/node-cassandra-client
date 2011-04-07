var assert = require('assert');
var console = require('console');
var Keyspace = require('../node-cassandra-client').Keyspace;
var System = require('../node-cassandra-client').System;
var KsDef = require('../node-cassandra-client').KsDef;
var CfDef = require('../node-cassandra-client').CfDef;
var Pool = require('../lib/pool').Pool;

/**
 * This test assumes you have cassandra running on 127.0.0.1:9160 (of course, other hosts are allowed too).
 */


// we'll need these in a bit.
var client;

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
  client = new Keyspace('Keyspace1', ['127.0.0.1:9160', '127.0.0.2:9160', '127.0.0.3:9160']);
  maybeCreateKeyspace(callback);
};

/** simple inserts. */
exports['testSimpleInsert'] =function() {
  var ts = 0;
  for (var i = 0; i < 10; i++) {
    client.insert('key0', 'Standard1', null, 'cola', 'valuea' + ts, ts++, function(err) {
      assert.ifError(err);
    });
  }
};

/** simple super inserts */
exports['testSuperInserts'] = function() {
  var ts = 0;
  for (i = 0; i < 10; i++) {
    for (j = 0; j < 10; j++) {
      client.insert('key0', 'Super1', 'super' + i, 'cola', 'valuea' + ts, ts++, function(err) {
        assert.ifError(err);
      });
    }
  }
};

/** closes the connection */
exports['tearDown'] = function(callback) {
  assert.ok(client);
  client.close(callback);
};
