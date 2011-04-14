// put thrift on the path so that we can require('thrift')

var console = require('console');
var sys = require('sys');
var EventEmitter = require('events').EventEmitter;

var Cassandra = require('./lib/gen-nodejs/Cassandra');
var ttypes = require('./lib/gen-nodejs/cassandra_types');
var Pool = require('./lib/pool').Pool;

// re-export the standard thrift structures.
var CfDef = module.exports.CfDef = ttypes.CfDef;
var KsDef = module.exports.KsDef = ttypes.KsDef;

var Connection = require('./lib/driver').Connection;

/** Naïve FIFO queue */
function Queue(maxSize) {
  var items = [];
  var putPtr = 0;
  var takePtr = 0;
  var max = maxSize;
  var curSize = 0;

  this.put = function(obj) {
    if (curSize == max) {
      return false;
    }
    if (items.length < max) {
      items.push(obj);
    }
    else {
      items[putPtr] = obj;
    }
    putPtr = (putPtr + 1) % max;
    curSize += 1;
    return true;
  };

  this.take = function() {
    if (curSize === 0) {
      return false;
    }
    var item = items[takePtr];
    items[takePtr] = null;
    takePtr = (takePtr + 1) % max;
    curSize -= 1;
    return item;
  };

  this.size = function() {
    return curSize;
  };
}

/** system database */
System = module.exports.System = function(urn) {
  EventEmitter.call(this);
  this.q = new Queue(500);
  this.pool = new Pool([urn]);
  
  var self = this;
  this.on('checkq', function() {
    var con = self.pool.getNext();
    if (!con) {
      // no connection is available. create a timer event to check back in a bit to see if there is a con available to
      // do work.
      setTimeout(function() {
        self.emit('checkq');
      }, 25);
    } else {
      // drain the work queue.
      while (self.q.size() > 0) {
        self.q.take()(con);
      }
    }
  });
  
  this.q.put(function(con) {
    con.thriftCli.set_keyspace('system', function(err) {
      if (err) {
        throw Error(err);
      }
    });
  });
};
sys.inherits(System, EventEmitter);

/** adds a keyspace */
System.prototype.addKeyspace = function(ksDef, callback) {
  this.q.put(function(con) {
    con.thriftCli.system_add_keyspace(ksDef, callback);
  });
  this.emit('checkq');
};

/** gets keyspace information */
System.prototype.describeKeyspace = function(ksName, callback) {
  this.q.put(function(con) {
    con.thriftCli.describe_keyspace(ksName, callback);
  });
  this.emit('checkq');
};

/** shuts down thrift connection */
System.prototype.close = function(callback) {
  self = this;
  this.q.put(function() {
    self.pool.tearDown();
    if (callback) {
      callback();
    }
  });
  this.emit('checkq');
};

function chopTrailingComma(str) {
  if (str.charAt(str.length - 1) === ',') {
    str = str.substr(0, str.length - 1);
  }
  return str;
}

// todo: pool connections.

/** ColumnFamily wrapper (uses the CQL driver) */
ColumnFamily = module.exports.ColumnFamily = function(keyspace, columnFamily, user, pass, host, port) {
  this.connection = new Connection(user, pass, host, port, keyspace);
  this.cfName = columnFamily;
};

ColumnFamily.prototype.insert = function(key, columns, timestamp, consistency, callback) {
  // UPDATE <COLUMN FAMILY> [USING CONSISTENCY <CL>] SET name1 = value1, name2 = value2 WHERE KEY = keyname;
  var stmt = this.connection.createStatement();
  var str = 'UPDATE ' + this.cfName + ' USING CONSISTENCY ' + consistency + ' SET ';
  for (var name in columns) {
    str += '\'' + name + '\'=\'' + columns[name] + '\',';
  }
  str = chopTrailingComma(str);
  str += ' WHERE KEY=\'' + key + '\'';
  console.log(str);
  stmt.update(str, callback);
};

ColumnFamily.prototype.remove = function(key, columns, consistency, callback) {
  // DELETE [COLUMNS] FROM <COLUMN FAMILY> [USING <CONSISTENCY>] WHERE KEY = keyname1
  var stmt = this.connection.createStatement();
  var str = 'DELETE ';
  for (var i = 0; i < columns.length; i++) {
    str += '\'' + columns[i] + '\',';
  }
  str = chopTrailingComma(str);
  str += ' FROM ' + this.cfName + ' USING CONSISTENCY ' + consistency + ' WHERE KEY=\'' + key + '\'';
  console.log(str);
  stmt.update(str, callback);
};

ColumnFamily.prototype.close = function() {
  this.connection.close();
};