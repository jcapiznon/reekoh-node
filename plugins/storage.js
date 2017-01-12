'use strict'

let async = require('async')
let hasProp = require('lodash.has')
let isEmpty = require('lodash.isempty')
// let isString = require('lodash.isstring')

let Promise = require('bluebird')
let Broker = require('../lib/broker.lib.js')
let EventEmitter = require('events').EventEmitter

class Storage extends EventEmitter {

  constructor (config) {
    super()

    this.config = {}
    this.queues = []
    this.loggers = []
    this.exceptionLoggers = []

    this.QN_GENERIC_LOGS = 'generic.logs'
    this.QN_GENERIC_DEVICES = 'generic.devices'
    this.QN_GENERIC_EXCEPTIONS = 'generic.exceptions'

    let _self = this
    let _broker = new Broker()
    let _brokerConnStr = process.env.BROKER || 'amqp://guest:guest@127.0.0.1/'

    let _config = proces.env.CONFIG || {}
    let _loggerIDs = process.env.LOGGERS || ''
    let _exceptionLoggerIDs = process.env.EXCEPTION_LOGGERS || ''
    let _pluginID = process.env.PLUGIN_ID || 'demo.dev-sync'

    //  preparing logger queue names in array
    _self.loggers = _loggerIDs.split(',')
    _self.exceptionLoggers = _exceptionLoggerIDs.split(',')
    _self.exceptionLoggers.push(_self.QN_GENERIC_EXCEPTIONS)
    _self.loggers.push(_self.QN_GENERIC_LOGS)



    async.waterfall([

      // parse config asynchronously
      (done) => {
        async.waterfall([
          async.constant(_config.toString('utf8')),
          async.asyncify(JSON.parse)
        ], (err, parsedConfig) => {
          _self.config = parsedConfig
          done(err)
        })
      },

      // connecting to rabbitMQ
      (done) => {
        return _broker.connect(_brokerConnStr)
          .then(() => {
            console.log('Connected to RabbitMQ Server.')
            return done() || null // !
          }).catch((err) => {
            done(err)
          })
      },

      // setting up messaging queues
      (done) => {
        let queueIDs = [
          _pluginID,
          _self.QN_GENERIC_DEVICES
        ]

        queueIDs = queueIDs
          .concat(_self.loggers)
          .concat(_self.exceptionLoggers)

        async.each(queueIDs, (loggerId, callback) => {
          if (isEmpty(loggerId)) return callback()

          _broker.newQueue(loggerId)
            .then((queue) => {
              _self.queues[loggerId] = queue
              return callback() || null // !
            }).catch((err) => {
            console.error('newQueue:', err)
          })
        }, (err) => {
          if (err) return console.error(err)
          return done()
        })
      },

      // process/listen for queued items
      (done) => {
        let processQueue = (msg) => {
          if (!isEmpty(msg)) {
            async.waterfall([
              async.constant(msg.content.toString('utf8')),
              async.asyncify(JSON.parse)
            ], (err, task) => {
              if (err) return console.error(err)
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

        _self.queues[_pluginID].consume(processQueue)
          .then((msg) => {
            console.log('Consuming:', msg)
            return done()
          }).catch((err) => {
          console.error(err)
        })
      }

      // plugin initialized
    ], (err) => {
      if (err) return console.error(err)
      _self.emit('ready')
    })
  }

  log (logData) {
    let self = this

    return new Promise((resolve, reject) => {
      if (isEmpty(logData)) return reject(new Error('Kindly specify the data to log'))

      // loggers and custom loggers are in self.loggers array
      async.each(self.loggers, (loggerId, callback) => {
        if (isEmpty(loggerId)) return callback()

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
      async.each(self.exceptionLoggers, (loggerId, callback) => {
        if (isEmpty(loggerId)) return callback()

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

module.exports = Storage
