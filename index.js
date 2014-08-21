/**
 * Module dependencies
 */
var Writable = require('stream').Writable;
var _ = require('lodash');
var TokenCache = require('google-oauth-jwt').TokenCache;
var request = require('request');
var tokens = new TokenCache();
/**
 * skipper-gcs
 *
 * @param  {Object} globalOpts
 * @return {Object}
 */
module.exports = function GCSStore(globalOpts) {
  globalOpts = globalOpts || {};
  _.defaults(globalOpts, {
    email: '',
    keyFile: '',
    bucket: '',
    scopes: ['https://www.googleapis.com/auth/devstorage.full_control']
  });
  var adapter = {
    ls: function(dirname, cb) { return cb(new Error('TODO')); },
    read: function(fd, cb) { return cb(new Error('TODO')); },
    rm: function(fd, cb) { return cb(new Error('TODO')); },
    /**
     * A simple receiver for Skipper that writes Upstreams to Google Cloud Storage
     *
     * @param  {Object} options
     * @return {Stream.Writable}
     */
    receive: function GCSReceiver(options) {
      options = options || {};
      options = _.defaults(options, globalOpts);
      var receiver__ = Writable({
        objectMode: true
      });
      // This `_write` method is invoked each time a new file is received
      // from the Readable stream (Upstream) which is pumping filestreams
      // into this receiver.  (filename === `__newFile.filename`).
      receiver__._write = function onFile(__newFile, encoding, done) {
        tokens.get({ email: options.email, keyFile: options.keyFile, scopes: options.scopes }, function(err, token) {
          // console.log('oauth token: ', token);
          var filename = typeof options.filename === 'undefined' ? __newFile.fd : options.filename + '.' + __newFile.filename.split('.').pop();
          var filepath = typeof options.path === 'undefined' ? filename : options.path + '/' + filename;
          var url = 'https://www.googleapis.com/upload/storage/v1/b/' + options.bucket + '/o?uploadType=media&name=' + encodeURIComponent(filepath);
          // console.log('file post url: ', url);
          __newFile.pipe(request.post({
            url: url,
            headers: { Authorization: 'Bearer ' + token, 'x-goog-api-version': 2 }
          }, function(err, res, body) {
            if (err) {
              receiver__.emit('error', err);
            } else {
              // console.log('upload response body: ', body);
              __newFile.extra = body;
              if (options.publicly) {
                request.post({
                  url: 'https://www.googleapis.com/storage/v1/b/' + options.bucket + '/o/' + encodeURIComponent(filepath) + '/acl',
                  headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token, 'x-goog-api-version': 2 },
                  json: { 'entity': 'allUsers', 'role': 'READER' }
                }, function(err, res, body) {
                  if (err) {
                    receiver__.emit('error', err);
                  } else {
                    // console.log('acl response body: ', body);
                    done();
                  }
                });
              } else {
                done();
              }
            }
          }));
        });
      }
      return receiver__;
    }
  };
  return adapter
};