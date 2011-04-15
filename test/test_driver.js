
var assert = require('assert');
var console = require('console');

var BigInteger = require('../lib/bigint').BigInteger;

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
      var super1 = new CfDef({keyspace: ksName, name: 'Super1', column_type: 'Super', comparator_type: 'UTF8Type', subcomparator_type: 'UTF8Type'});
      var keyspace1 = new KsDef({name: ksName, strategy_class: 'org.apache.cassandra.locator.SimpleStrategy', strategy_options: {'replication_factor': '1'}, cf_defs: [standard1, super1, cfInt, cfUtf8, cfLong, cfBytes, cfUuid]});
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

exports['testInvalidUpdate'] = function() {
  var con = connect();
  var stmt = con.createStatement();
  stmt.update('select \'cola\' from Standard1 where key=' + stringToHex('key0', true), function(err) {
    con.close();
    assert.notEqual(err, null);
  });
};

exports['testSimpleUpdate'] = function() {
  var con = connect();
  var stmt = con.createStatement();
  var key = stringToHex('key0', true);
  stmt.update('update Standard1 set \'cola\'=\'valuea\', \'colb\'=\'valueb\' where key=' + key, function(updateErr) {
    if (updateErr) {
      con.close();
      throw new Error(updateErr);
    }
    // verify the query succeeded.
    stmt.query('select \'cola\', \'colb\' from Standard1 where key=' + key, function(selectErr, res) {
      con.close();
      if (selectErr) {
        throw new Error(selectErr);
      }
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
  var con = connect();
  var stmt = con.createStatement();
  var key = stringToHex('key1', true);
  stmt.update('update Standard1 set \'colx\'=\'xxx\', \'colz\'=\'bbb\' where key=' + key, function(updateErr) {
    if (updateErr) {
      con.close();
      throw new Error(updateErr);
    }
    stmt.update('delete \'colx\', \'colz\' from Standard1 where key=' + key, function(deleteErr) {
      con.close();
      if (deleteErr) {
        throw new Error(deleteErr);
      }
    });
  });
};

exports['testLong'] = function() {
  var con = connect();
  var stmt = con.createStatement();
  // the third pair is ±2^62, which overflows the 53 bits in the fp mantissa js uses for numbers (should lose precision
  // coming back), but still fits nicely in an 8-byte long (it should work).
  stmt.update('update CfLong set \'1\'=\'2\', \'3\'=\'4\', \'4611686018427387904\'=\'-4611686018427387904\' where key=\'12345\'', function(updateErr) {
    if (updateErr) {
      con.close();
      throw new Error(updateErr);
    } else {
      stmt.query('select \'1\', \'3\', \'4611686018427387904\' from CfLong where key=\'12345\'', function(selectErr, res) {
        con.close();
        if (selectErr) {
          throw new Error(selectErr);
        }
        assert.ok(res.next());
        
        // getting by index is easy.
        assert.equal('1', res.getByIndex(0).name.toString());
        assert.equal('2', res.getByIndex(0).value.toString());
        assert.equal('3', res.getByIndex(1).name.toString());
        assert.equal('4', res.getByIndex(1).value.toString());
        assert.ok(new BigInteger('4611686018427387904').equals(res.getByIndex(2).name));
        assert.ok(new BigInteger('-4611686018427387904').equals(res.getByIndex(2).value));
        
        // getting by column name is harder.
        assert.equal('2', res.getByName(1).toString());
        assert.equal('4', res.getByName(3).toString());
        assert.ok(new BigInteger('-4611686018427387904').equals(res.getByName(new BigInteger('4611686018427387904'))));
        
        assert.ok(!res.next());
      });
    }
  });
};

exports['testInt'] = function() {
  var con = connect();
  var stmt = con.createStatement();
  // make sure to use some numbers that will overflow a 64 bit signed value.
  stmt.update('update CfInt set \'1\'=\'11\', \'-1\'=\'-11\', \'8776496549718567867543025521\'=\'-8776496549718567867543025521\' where key=\'3456543434345654345332453455633\'', function(updateErr) {
    if (updateErr) {
      con.close();
      throw new Error(updateErr);
    } else {
      stmt.query('select \'-1\', \'1\', \'8776496549718567867543025521\' from CfInt where key=\'3456543434345654345332453455633\'', function(selectErr, res) {
        con.close();
        if (selectErr) {
          throw new Error(selectErr);
        }
        assert.ok(res.next());
        
        // by index
        assert.ok(new BigInteger('-1').equals(res.getByIndex(0).name));
        assert.ok(new BigInteger('-11').equals(res.getByIndex(0).value));
        assert.ok(new BigInteger('1').equals(res.getByIndex(1).name));
        assert.ok(new BigInteger('11').equals(res.getByIndex(1).value));
        assert.ok(new BigInteger('8776496549718567867543025521').equals(res.getByIndex(2).name));
        assert.ok(new BigInteger('-8776496549718567867543025521').equals(res.getByIndex(2).value));
        
        // by name
        assert.ok(new BigInteger('-11').equals(res.getByName(new BigInteger('-1'))));
        assert.ok(new BigInteger('11').equals(res.getByName(new BigInteger('1'))));
        assert.ok(new BigInteger('-8776496549718567867543025521').equals(res.getByName(new BigInteger('8776496549718567867543025521'))));
        
        assert.ok(!res.next());
      });
    }
  });
};

exports['testUUID'] = function() {
  // make sure we're not comparing the same things.
  assert.ok(!new UUID('string', '6f8483b0-65e0-11e0-0000-fe8ebeead9fe').equals(new UUID('string', '6fd589e0-65e0-11e0-0000-7fd66bb03aff')));
  assert.ok(!new UUID('string', '6fd589e0-65e0-11e0-0000-7fd66bb03aff').equals(new UUID('string', 'fa6a8870-65fa-11e0-0000-fe8ebeead9fd')));
  var con = connect();
  var stmt = con.createStatement();
  stmt.update('update CfUuid set \'6f8483b0-65e0-11e0-0000-fe8ebeead9fe\'=\'6fd45160-65e0-11e0-0000-fe8ebeead9fe\', \'6fd589e0-65e0-11e0-0000-7fd66bb03aff\'=\'6fd6e970-65e0-11e0-0000-fe8ebeead9fe\' where key=\'fa6a8870-65fa-11e0-0000-fe8ebeead9fd\'', function(updateErr) {
    if (updateErr) {
      con.close();
      throw new Error(updateErr);
    } else {
      stmt.query('select \'6f8483b0-65e0-11e0-0000-fe8ebeead9fe\', \'6fd589e0-65e0-11e0-0000-7fd66bb03aff\' from CfUuid where key=\'fa6a8870-65fa-11e0-0000-fe8ebeead9fd\'', function(selectErr, res) {
        con.close();
        if (selectErr) {
          throw new Error(selectErr);
        }
        assert.ok(res.next());
        assert.ok(new UUID('string', '6f8483b0-65e0-11e0-0000-fe8ebeead9fe').equals(res.getByIndex(0).name));
        assert.ok(new UUID('string', '6fd45160-65e0-11e0-0000-fe8ebeead9fe').equals(res.getByIndex(0).value));
        assert.ok(new UUID('string', '6fd589e0-65e0-11e0-0000-7fd66bb03aff').equals(res.getByIndex(1).name));
        assert.ok(new UUID('string', '6fd6e970-65e0-11e0-0000-fe8ebeead9fe').equals(res.getByIndex(1).value));
        
        assert.ok(res.getByName(new UUID('string', '6fd589e0-65e0-11e0-0000-7fd66bb03aff')).equals(res.getByIndex(1).value));
        assert.ok(res.getByName(res.getByIndex(0).name).equals(res.getByIndex(0).value));
        assert.ok(res.getByName(new UUID('string', '6f8483b0-65e0-11e0-0000-fe8ebeead9fe')).equals(res.getByIndex(0).value));
        assert.ok(res.getByName(res.getByIndex(1).name).equals(res.getByIndex(1).value));
        
        assert.ok(!res.next());
      });
    }
  });
};

exports['testExecute'] = function() {
  var con = connect();
  con.execute('update CfLong set ?=?,?=?,?=? where key=?', [1,2,3,4,5,6,7], function(updErr) {
    if (updErr) {
      con.close();
      throw new Error(updErr);
    } else {
      con.execute('select ?,?,? from CfLong where key=?', [1,3,5, 7], function(selErr, row) {
        if (selErr) {
          con.close();
          throw new Error(selErr);
        } else {
          assert.strictEqual(row.colCount(), 3);
          assert.ok(new BigInteger('2').equals(row.colHash[1]));
          assert.ok(new BigInteger('4').equals(row.colHash[3]));
          assert.ok(new BigInteger('6').equals(row.colHash[5]));
          con.close();
        }
      });
    }
  });
}


//this is for running some of the tests outside of whiskey.
maybeCreateKeyspace(function() {
  exports.testExecute();
});
