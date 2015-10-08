/**
 * Module dependencies
 */
var debug = require('debug')('skipper-gcs');
var qs = require('querystring');
var Writable = require('stream').Writable;
var _ = require('lodash');
var gcloud = require('gcloud');
var concat = require('concat-stream');
var mime = require('mime');

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
    bucket: '',
    scopes: ['https://www.googleapis.com/auth/devstorage.full_control']
  });
  var gcs = gcloud.storage({
    projectId: globalOpts.projectId,
    keyFilename: globalOpts.keyFilename
  });
  var bucket = gcs.bucket(globalOpts.bucket);

  var adapter = {
    ls: function(dirname, cb) {
      bucket.getFiles({ prefix: dirname }, function(err, files) {
        if (err) {
          cb(err)
        } else {
          files = _.pluck(files, 'name');
          cb(null, files);
        }
      });
    },
    read: function(fd, cb) {
      var remoteReadStream = bucket.file(fd).createReadStream();
      remoteReadStream
        .on('error', function(err) {
          cb(err);
        })
        .on('response', function(response) {
          // Server connected and responded with the specified status and headers.
        })
        .on('end', function() {
          // The file is fully downloaded.
        })
        .pipe(concat(function(data) {
          cb(null, data);
        }));
    },
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
        var metadata = {};
        _.defaults(metadata, options.metadata);
        metadata.contentType = mime.lookup(__newFile.fd);
        var file = bucket.file(__newFile.fd);
        var stream = file.createWriteStream({
          metadata: metadata
        });
        stream.on('error', function(err) {
          receiver__.emit('error', err);
          return;
        })
        stream.on('finish', function() {
          __newFile.extra = file.metadata;
          __newFile.extra.Location = 'https://storage.googleapis.com/' + globalOpts.bucket + '/' + __newFile.fd;
          if(globalOpts.public) file.makePublic();
          done();
        });
        __newFile.pipe(stream);
      }
      return receiver__;
    }
  };
  return adapter
};
