
var kafka = require('kafka-node')
var _ = require('underscore')

var util = require('util')
var EventEmitter = require('events').EventEmitter


function FullDuplexClient(kafkaClient, publishTopic, subscribeTopic, partition) {
  EventEmitter.call(this)

  this._kafkaClient = kafkaClient
  this._publishTopic = publishTopic
  this._subscribeTopic = subscribeTopic
  this._partition = partition

  var self = this

  this.initialize(function(err) {

    self.emit('ready', err)
  })

}

util.inherits(FullDuplexClient, EventEmitter)

FullDuplexClient.prototype.initialize = function(callback) {
  var self = this
  console.log('initializing publisher channel')
  self._initializePublisher(function(err) {
    if(err) {
      console.log('failed to initialize publisher channel')
      console.log(err)
      callback(err)
    } else {
      console.log('publisher initialized')

      self._producer.createTopics([self._publishTopic, self._subscribeTopic], function (err, data) {
        if(err) {
          console.log('failed to create topics')
          console.log(err)
          callback(err)
        } else {
          console.log('created topics', data)

          self._initializeSubscriber(function(err) {
            if(err) {
              console.log('failed to initialize subscriber channel')
              console.log(err)
              callback(err)
            } else {
              callback(undefined)
            }
          })
        }
      })
    }
  })
}

FullDuplexClient.prototype._initializePublisher = function(callback) {

  this._producer = new kafka.Producer(this._kafkaClient)
  this._producer.on('ready', function () {
    console.log('producer ready')
    callback(undefined)
  })
}

FullDuplexClient.prototype._initializeSubscriber = function(callback) {

  var self = this

  this._consumer = new kafka.Consumer(
    self._kafkaClient, [
      { topic: self._subscribeTopic, partition: self._partition }
    ], {
      autoCommit: true
    }
  )

  this._consumer.on('message', function (message) {
    var value = JSON.parse(message.value)
    if(_.isArray(value)) {
      for(var i = 0 ; i < value.length ; i++) {
        self.emit('message', value[i])
      }
    } else {
      self.emit('message', value)
    }
  })
  this._consumer.on('error', function (err) {
    console.log(err)
  })

  // TODO: on.('error', ...)

  setImmediate(callback)
}

FullDuplexClient.prototype.send = function(data) {
  var msg = {
    topic: this._publishTopic,
    messages: JSON.stringify(data),
    partition: this._partition
  }
  this._producer.send([msg])
}



module.exports = FullDuplexClient
