/**
 * Wrap a function so that the original function will only be called once,
 * regardless of how  many times the wrapper is called.
 * @param {Function} fn The to wrap.
 * @return {Function} A function which will call fn the first time it is called.
 */
exports.fireOnce = function fireOnce(fn) {
  var fired = false;
  return function wrapped() {
    if (!fired) {
      fired = true;
      fn.apply(null, arguments);
    }
  };
};
