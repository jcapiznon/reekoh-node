'use strict'

const async = require('async')
const hasProp = require('lodash.has')
const isEmpty = require('lodash.isempty')

const Promise = require('bluebird')
const Broker = require('../lib/broker.lib.js')
const EventEmitter = require('events').EventEmitter

class DeviceSync extends EventEmitter {

  constructor () {
    super()

    this.config = {}
    this.queues = []

    this.QN_AGENT_DEVICES = 'agent.devices'
    this.QN_PLUGIN_ID = process.env.PLUGIN_ID || 'demo.dev-sync'

    this.qn = {
      common: [
        this.QN_PLUGIN_ID,
        this.QN_AGENT_DEVICES
      ],
      loggers: ['agent.logs'],
      exceptionLoggers: ['agent.exceptions']
    }

    let _self = this
    let _broker = new Broker()

    let _config = process.env.CONFIG || '{}'
    let _loggerIDs = process.env.LOGGERS || ''
    let _exLoggerIDs = process.env.EXCEPTION_LOGGERS || ''
    let _brokerConnStr = process.env.BROKER || 'amqp://guest:guest@127.0.0.1/'

    // preparing logger queue names in array
    _self.qn.exceptionLoggers = _self.qn.exceptionLoggers.concat(_exLoggerIDs.split(','))
    _self.qn.loggers = _self.qn.loggers.concat(_loggerIDs.split(','))

    // removing empty elements in any (guard)
    _self.qn.exceptionLoggers = _self.qn.exceptionLoggers.filter(Boolean)
    _self.qn.loggers = _self.qn.loggers.filter(Boolean)

    async.waterfall([

      // parse config trap error
      (done) => {
        async.waterfall([
          async.constant(_config.toString('utf8')),
          async.asyncify(JSON.parse)
        ], (err, parsed) => {
          _self.config = parsed
          done(err)
        })
      },

      // connecting to rabbitMQ
      (done) => {
        _broker.connect(_brokerConnStr)
          .then(() => {
            // console.log('Connected to RabbitMQ Server.')
            return done() || null // !
          }).catch((err) => {
            done(err)
          })
      },

      // setting up generic queues
      (done) => {
        let queueIDs = []

        queueIDs = queueIDs.concat(_self.qn.common)
        queueIDs = queueIDs.concat(_self.qn.loggers)
        queueIDs = queueIDs.concat(_self.qn.exceptionLoggers)

        async.each(queueIDs, (loggerId, callback) => {
          if (isEmpty(loggerId)) return callback()

          _broker.newQueue(loggerId)
            .then((queue) => {
              if (queue) _self.queues[loggerId] = queue
              return callback() || null // !
            }).catch((err) => {
              console.error('DeviceSync newQueue() ', err)
            })
        }, (err) => {
          done(err)
        })
      },

      // process/listen for queued items
      (done) => {
        let processTask = (msg) => {
          if (!isEmpty(msg)) {
            async.waterfall([
              async.constant(msg.content.toString('utf8')),
              async.asyncify(JSON.parse)
            ], (err, task) => {
              if (err) return console.error('DeviceSync processQueue() rcvd data is not a JSON.', err)

              if (task.operation === 'sync') {
                _self.emit('sync')
              } else if (task.operation === 'adddevice') {
                _self.emit('adddevice', task.device)
              } else if (task.operation === 'updatedevice') {
                _self.emit('updatedevice', task.device)
              } else if (task.operation === 'removedevice') {
                _self.emit('removedevice', task.device)
              }
            })
          }
        }

        let queueName = _self.QN_PLUGIN_ID
        _self.queues[queueName].consume(processTask)
          .then((queueInfo) => {
            // console.log('DeviceSync Consuming:', queueInfo)
            return done()
          }).catch((err) => {
            done(err)
          })
      }

    ], (err) => {
      if (err) return console.error('DeviceSync:', err)

      // plugin initialized
      _self.emit('ready')
    })
  }

  syncDevice (deviceInfo) {
    let queueName = this.QN_AGENT_DEVICES
    let queue = this.queues[queueName]

    return new Promise((resolve, reject) => {
      if (isEmpty(deviceInfo)) {
        return reject(new Error('Kindly specify the device information/details'))
      }
      if (!(hasProp(deviceInfo, '_id') || hasProp(deviceInfo, 'id'))) {
        return reject(new Error('Kindly specify a valid id for the device'))
      }
      if (!hasProp(deviceInfo, 'name')) {
        return reject(new Error('Kindly specify a valid name for the device'))
      }

      let message = JSON.stringify({
        operation: 'sync',
        data: deviceInfo
      })

      queue.publish(message)
        .then(() => {
          resolve()
        }).catch((err) => {
          reject(err)
        })
    })
  }

  removeDevice (deviceId) {
    let queue = this.queues[this.QN_AGENT_DEVICES]

    return new Promise((resolve, reject) => {
      if (!deviceId) return reject(new Error('Kindly specify the device identifier'))

      let message = JSON.stringify({
        operation: 'remove',
        data: {_id: deviceId}
      })

      queue.publish(message)
        .then(() => {
          resolve()
        }).catch((err) => {
          reject(err)
        })
    })
  }

  log (logData) {
    let self = this

    return new Promise((resolve, reject) => {
      if (isEmpty(logData)) return reject(new Error('Kindly specify the data to log'))

      // loggers and custom loggers are in self.loggers array
      async.each(self.qn.loggers, (loggerId, callback) => {
        if (!loggerId) return callback()

        // publish() has a built in stringify, so objects are safe to feed
        self.queues[loggerId].publish(logData)
          .then(() => {
            resolve()
          }).catch((err) => {
            reject(err)
          })
      }, (err) => {
        if (err) return reject(err)
        resolve()
      })
    })
  }

  logException (err) {
    let self = this

    return new Promise((resolve, reject) => {
      if (!(err instanceof Error)) return reject(new Error('Kindly specify a valid error to log'))

      let data = JSON.stringify({
        name: err.name,
        message: err.message,
        stack: err.stack
      })

      // exLoggers and custom exLoggers are in self.loggers array
      async.each(self.qn.exceptionLoggers, (loggerId, callback) => {
        if (!loggerId) return callback()

        self.queues[loggerId].publish(data)
          .then(() => {
            resolve()
          }).catch((err) => {
            reject(err)
          })
      }, (err) => {
        if (err) return reject(err)
        resolve()
      })
    })
  }

}

module.exports = DeviceSync
