// some tests for the uuid-js module.

var UUID = require('uuid-js');

exports['test_uuid_from_buffer'] = function(test, assert) {
  var buf = new Buffer('\u00ee\u00a1\u006c\u00c0\u00cf\u00bd\u0011\u00e0\u0017' +
          '\u000a\u00dd\u0026\u0075\u0027\u009e\u0008', 'binary');
  var uuid = UUID.fromBytes(buf);
  assert.strictEqual(uuid.toString(), 'eea16cc0-cfbd-11e0-170a-dd2675279e08');
  test.finish();
};

// this test currently doesn't work, but I'd like to see the work done in uuid-js to make it happen.  the problem is
// that it only generates time uuids for the beginning or ending of a specific millisecond.  It should support 
// generating multiple successive UUIDs for the same millisecond for highly concurrent applications.
exports['test_uuid_backwards_in_time'] = function(test, assert) {
  test.skip();
  
  var ts = 1314735336316;
  var uuidTs = UUID.fromTime(ts).toString();
  // this forces the nano tracker in uuid to get set way ahead.
  var uuidFuture = UUID.fromTime(ts + 5000).toString();
  // we want to verify that the nanos used reflect ts and not ts+5000.
  var uuidTsSame = UUID.fromTime(ts).toString();
  assert.ok(uuidTs !== uuidFuture); // duh
  assert.ok(uuidTs !== uuidTsSame); // generated from same TS after going back in time.
  // but time lo should definitely be the same.
  // this test would have failed before we started using the back-in-time reset block in UUID.nanos().
  assert.strictEqual(uuidTs.split('-')[0], uuidTsSame.split('-')[0]);
  test.finish();
};