
var assert = require('assert');
var console = require('console');
var Connection = require('../lib/driver').Connection;
var ttypes = require('../lib/gen-nodejs/cassandra_types');
var Keyspace = require('../node-cassandra-client').Keyspace;
var System = require('../node-cassandra-client').System;
var KsDef = require('../node-cassandra-client').KsDef;
var CfDef = require('../node-cassandra-client').CfDef;

function stringToHex(s, quote) {
  var buf = '';
  for (var i = 0; i < s.length; i++) {
    buf += s.charCodeAt(i).toString(16);
  }
  if (quote) {
    buf = '\'' + buf + '\'';
  }
  return buf;
}

function maybeCreateKeyspace(callback) {
  var sys = new System('127.0.0.1:9160');
  var ksName = 'Keyspace2';
  var close = function() {
    sys.close(function() {
      sys.close();
      console.log('System keyspace closed');
    });
  };
  sys.describeKeyspace(ksName, function(err, ksDef) {
    if (err) {
      var standard1 = new CfDef({keyspace: ksName, name: 'Standard1', column_type: 'Standard', comparator_type: 'UTF8Type', default_validation_class: 'UTF8Type'});
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

exports['setUp'] = function(callback) {
  maybeCreateKeyspace(callback);
};

exports['testInvalidUpdate'] = function() {
  var con = new Connection(null, null, '127.0.0.1', 9160, 'Keyspace2');
  var stmt = con.createStatement();
  stmt.update('select \'cola\' from Standard1 where key=' + stringToHex('key0', true), function(err) {
    con.close();
    assert.notEqual(err, null);
  });
};

exports['testSimpleUpdate'] = function() {
  var con = new Connection(null, null, '127.0.0.1', 9160, 'Keyspace2');
  var stmt = con.createStatement();
  var key = stringToHex('key0', true);
  stmt.update('update Standard1 set \'cola\'=\'valuea\', \'colb\'=\'valueb\' where key=' + key, function(updateErr) {
    if (updateErr) {
      con.close();
      assert.ok(false, updateErr);
    }
    // verify the query succeeded.
    stmt.query('select \'cola\', \'colb\' from Standard1 where key=' + key, function(selectErr, res) {
      con.close();
      assert.equal(null, selectErr);
      assert.ok(res.next());
      assert.equal('valuea', res.getByIndex(0).value);
      assert.equal('cola', res.getByIndex(0).name);
      assert.equal('valueb', res.getByIndex(1).value);
      assert.equal('colb', res.getByIndex(1).name);
      assert.equal('valuea', res.getByName('cola'));
      assert.equal('valueb', res.getByName('colb'));
      assert.ok(!res.next());
    });
  });
};

exports['testSimpleDelete'] = function() {
  var con = new Connection(null, null, '127.0.0.1', 9160, 'Keyspace2');
  var stmt = con.createStatement();
  var key = stringToHex('key1', true);
  stmt.update('update Standard1 set \'colx\'=\'xxx\', \'colz\'=\'bbb\' where key=' + key, function(updateErr) {
    if (updateErr) {
      con.close();
      assert.ok(false, updateErr);
    }
    stmt.update('delete \'colx\', \'colz\' from Standard1 where key=' + key, function(deleteErr) {
      con.close();
      assert.ifError(deleteErr);
    });
  });
};