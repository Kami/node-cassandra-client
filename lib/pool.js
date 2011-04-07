var console = require('console');
var thrift = require('thrift');
var Cassandra = require('./gen-nodejs/Cassandra');
var ttypes = require('./gen-nodejs/cassandra_types');

/** encapsulates a connection (thrift proto) and a client bound to it. */
Connection = module.exports.Connection = function(thriftCon, thriftCli) {
  this.thriftCon = thriftCon;
  this.thriftCli = thriftCli;
};

Connection.prototype.tearDown = function() {
  this.thriftCon.end();
};

/** A pool of thrift connections. */
Pool = module.exports.Pool = function Pool(urns) {
  this.connecting = [];
  this.connections = [];
  this.connectors = [];
  this.index = 0;
  var self = this;
  for (var i = 0; i < urns.length; i++) {
    this.connections[i] = null;
    this.connecting[i] = false;
    var parts = urns[i].split(':');
    // lazy connection strategy.
    // btw, JS scoping rules really bum me out.
    this.connectors[i] = (function(addr, port, index) {
      return function(callback) {
        self._make_client(addr, port, index, callback);
      };
    })(parts[0], parts[1], i);
  }
  this.length = i;
};

// makes the thrift connection+client.
Pool.prototype._make_client = function(addr, port, i, callback) {
  if (!this.connecting[i] && this.connections[i]) {
    if (callback) {
      callback(this.connections[i]);
    }
    return; // already connected.
  }
  this.connecting[i] = true;
  var con = thrift.createConnection(addr, port);
  var self = this;
  con.on('error', function(err) {
    console.error(err);
    self.connections[i] = null;
    self.connecting[i] = false;
  });
  var client = thrift.createClient(Cassandra, con);
  console.log('connected to ' + addr + ':' + port + '@' + i);
  self.connections[i] = new Connection(con, client);
  if (callback) {
    callback(this.connections[i]);
  }
  self.connecting[i] = false;
};

/** borrows a connection from the pool. not sophisticated. */
Pool.prototype.getNext = function() {
  var stop = (this.length + this.index - 1) % this.length;
  var ptr = this.index;
  do {
    // some connections may need to be started up.
    if (!this.connections[ptr] && !this.connecting[ptr]) {
      this.connectors[ptr]();
    }
    if (this.connections[ptr]) {
      break;
    }
    else {
      ptr = (ptr + 1) % this.length;
    }
  } while (ptr != stop);

  // only increment index once.
  this.index = (this.index + 1) % this.length;

  return this.connections[ptr]; // may be null!
};

Pool.prototype.forEach = function(fn) {
  for (var i = 0; i < this.length; i++) {
    this.connectors[i](fn);
  }
}

/** returns the connection and closes the physical link to the cassandra server */
Pool.prototype.tearDown = function() {
  for (var i = 0; i < this.length; i++) {
    if (this.connections[i]) {
      this.connections[i].tearDown();
    }
    this.connecting[i] = false;
    this.connections[i] = null;
  }
};