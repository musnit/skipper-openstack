var pkgcloud = require("pkgcloud-bluemix-objectstorage"),
    Writable = require("stream").Writable,
    _ = require("underscore");

function getClient(credentials) {

  var config = {
    provider: "openstack",
    authUrl: credentials.authUrl,
    useServiceCatalog: true,
    useInternal: false,
    tenantId: credentials.projectId,
    userId: credentials.userId,
    username: credentials.username,
    password: credentials.password,
    region: credentials.region
  };

  config.auth = {
    forceUri  : credentials.authUrl + '/v3/auth/tokens', //force uri to v3, usually you take the baseurl for authentication and add this to it /v3/auth/tokens (at least in bluemix)
    interfaceName : "public", //use public for apps outside bluemix and internal for apps inside bluemix. There is also admin interface, I personally do not know, what it is for.
    "identity": {
        "methods": [
            "password"
        ],
        "password": {
            "user": {
                "id": credentials.userId,
                "password": credentials.password
            }
        }
    },
    "scope": {
        "project": {
            "id": credentials.projectId
        }
    }
  };

  // Create a pkgcloud storage client
  return pkgcloud.storage.createClient(config);
}

module.exports = function SwiftStore(globalOpts) {
    globalOpts = globalOpts || {};

    var adapter = {

        read: function(options, file, response) {
            var client = getClient(options.credentials);
            client.download({
                container: options.container,
                remote: file,
                stream: response
            });
        },

        rm: function(fd, cb) { return cb(new Error('TODO')); },

        ls: function(options, callback) {
            var client = getClient(options.credentials);

            client.getFiles(options.container, function (error, files) {
                return callback(error, files);
            });
        },

        receive: function(options) {
            var receiver = Writable({
                objectMode: true
            });

            receiver._write = function onFile(__newFile, encoding, done) {
                var client = getClient(options.credentials);
                __newFile.pipe(client.upload({
                    container: options.container,
                    remote: __newFile.fd
                }, function(err, value) {
                  console.log(err);
                  console.log(value);
                    if( err ) {
                      console.log( err);
                      receiver.emit( 'error', err );
                      return;
                    }
                    done();
                }));

                __newFile.on("end", function(err, value) {
                    receiver.emit('finish', err, value );
                    done();
                });

            };

            return receiver;
        },
        ensureContainerExists: function(credentials, containerName, callback) {
          var client = getClient(credentials);

          client.getContainers(function (error, containers) {
            if (error) {
              callback(error);
              return;
            }
            if (containers.length === 0) {
              client.createContainer(containerName, callback);
            }
            else {
              var found = _.find(containers, function (container) {
                  return container.name === containerName;
              });
              if (found === undefined) {
                client.createContainer(containerName, callback);
              }
              else {
                callback(null);
              }
            }

          });
        }
    }

  return adapter;
}
