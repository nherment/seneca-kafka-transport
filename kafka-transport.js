'use strict';

var kafka = require('kafka-node');
var FullDuplexClient = require('./lib/FullDuplexClient.js')
var uuid = require('uuid')


module.exports = function( options ) {
  var seneca = this;
  var plugin = 'kafka-transport';


  options = seneca.util.deepextend({
    topicPrefix: 'seneca_',
    partition: 0,
    zkHost: 'localhost',
    zkPort: 2181,
    zkPath: '/'
  }, options);

  var requestTopic = options.topicPrefix + 'request'
  var responseTopic = options.topicPrefix + 'response'

  function kafkaClient(){
    return new kafka.Client(options.zkHost + ':' + options.zkPort + options.zkPath, uuid.v4(), {
      sessionTimeout: 10000,
      spinDelay : 1000,
      retries : 3
    })
  }

  var callMap = {}

  var client
  var listener


  if (!seneca.hasplugin('transport')) {
    seneca.use('transport');
  }

  function initializeClient(callback) {
    if(client) {
      callback(undefined)
    } else {
      var c = new FullDuplexClient(kafkaClient(), requestTopic, responseTopic)
      c.on('ready', function(err) {
        if(err) {
          callback(err)
        } else {
          client = c

          client.on('message', function(message) {
            var call = callMap[message.id];
            if( call ) {
              console.log('processing external response >', message.id)
              delete callMap[message.id];
              call(message.err ? new Error(message.err) : null, message.res);
            }
          })

          callback(undefined)
        }
      })
    }

  }

  function resumeSenecaContext(globalSeneca, args) {
    var augmentedParams = {}
    for(var attr in args) {
      if(~attr.indexOf('$')) {
        augmentedParams[attr] = args[attr]
      }
    }
    return globalSeneca.delegate(augmentedParams)
  }


  function hookListenQueue(args, done) {
    var seneca = this;

    if(listener) {
      done(undefined)
    } else {
      var l = new FullDuplexClient(kafkaClient(), responseTopic, requestTopic)
      l.on('ready', function(err) {
        if(err) {
          console.log(err)
          done(err)
        } else {
          listener = l
          setImmediate(done)

          listener.on('message', function(message) {
            console.log('received external request <', message.id)
            if(message.kind === 'act') {


              var s = resumeSenecaContext(seneca, message.act)

              s.act(message.act, function(err, res){
                if(err) {
                  // I do not want to magically swallow the stack trace so I print it here
                  // TODO: ideally it should be passed to the client
                  console.log(err)
                }
                var outmsg = {
                  id   : message.id,
                  err  : err ? err.message : null,
                  res  : res
                }
                console.log('responding to external request >', message.id)
                listener.send([outmsg])
              })
            }

          })

        }
      })
    }
  }



  /**
   * TODO: topic name to come from config
   */
  function hookClientQueue( args, done ) {
    var seneca = this

    initializeClient(function(err) {
      if(err) {
        done(err, undefined)
      } else {
        done(undefined, function(args, callback) {
          var messageId = uuid.v4()

          callMap[messageId] = callback

          var outMsg = {
            id:   messageId,
            kind: 'act',
            act:  args
          }

          console.log('sending external request>', outMsg.id)

          client.send([outMsg])
        })
      }

    })


  }

  var shutdown = function(args, done) {
    client.close(done)
  };

  seneca.add({role:'transport',hook:'listen',type:'queue'}, hookListenQueue);
  seneca.add({role:'transport',hook:'client',type:'queue'}, hookClientQueue);
  seneca.add({role:'seneca',cmd:'close'}, shutdown);

  return {
    name: plugin,
  };
};

