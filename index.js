/**
 * Module dependencies
 */
var debug = require('debug')('skipper-gcs');
var qs = require('querystring');
var Writable = require('stream').Writable;
var _ = require('lodash');
var request = require('request');
var token = require('./standalone/token');
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
      // console.log(dirname);
      bucket.getFiles({ prefix: dirname}, function(err, files) {
        if (err) {
          cb(err)
        } else {
          // console.log(files);
          files = _.pluck(files, 'name');
          // console.log(files);
          cb(null, files);
        }
      });
      // return cb(new Error('TODO'));
    },
    read: function(fd, cb) {
      var remoteReadStream = bucket.file(fd).createReadStream();
      remoteReadStream.pipe(concat(function (data) {
          // if (firedCb) return;
          // firedCb = true;
          cb(null, data);
        }));
      // return cb(new Error('TODO'));
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
        // token.cache({ email: options.email, keyFile: options.keyFile, scopes: options.scopes }, function(err, token) {
        //   if (err) {
        //     // console.log('request token error' + err);
        //     receiver__.emit('error', err);
        //   } else {
        //     debug('token object', token);
        //     var metadata = { name: __newFile.fd };
        //     _.defaults(metadata, options.metadata);
        //     debug('metadata object', metadata);
        //     var querystring = { uploadType: 'multipart' };
        //     _.defaults(querystring, options.querystring);
        //     var url = 'https://www.googleapis.com/upload/storage/v1/b/' + options.bucket + '/o?' + qs.stringify(querystring);
        //     debug('url', url);
        //     request.post({
        //       preambleCRLF: true,
        //       postambleCRLF: true,
        //       url: url,
        //       multipart: [
        //         { 'Content-Type':'application/json', body: JSON.stringify(metadata) },
        //         { body: __newFile }
        //       ],
        //       headers: { Authorization: 'Bearer ' + token.access_token }
        //     }, function(err, res, body) {
        //       if (err) {
        //         console.log('connect error.', err);
        //         receiver__.emit('error', err);
        //       } else {
        //         debug('upload response');
        //         debug('http status', res.statusCode);
        //         debug('body', body);
        //         var extra = JSON.parse(body);
        //         if(extra.error) {
        //           console.log('file upload error.', err);
        //           receiver__.emit('error', extra.error);
        //         } else {
        //           __newFile.extra = JSON.parse(body);
        //           done();
        //         }
        //       }
        //     });
        //   }
        // });
      }
      return receiver__;
    }
  };
  return adapter
};
