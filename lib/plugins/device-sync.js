'use strict'

const async = require('async')
const BPromise = require('bluebird')
const EventEmitter = require('events').EventEmitter

const hasProp = require('lodash.has')
const isError = require('lodash.iserror')
const isEmpty = require('lodash.isempty')
const isString = require('lodash.isstring')
const isPlainObject = require('lodash.isplainobject')

const Broker = require('../broker.lib')

class DeviceSync extends EventEmitter {

  constructor () {
    super()
    this.config = {}

    const ENV_ACCOUNT = process.env.ACCOUNT || ''

    const QN_LOGS = 'logs'
    const QN_DEVICES = 'devices'
    const QN_EXCEPTIONS = 'exceptions'
    const QN_PLUGIN_ID = process.env.PLUGIN_ID || 'demo.dev-sync'

    let _broker = new Broker()

    let _config = process.env.CONFIG || '{}'
    let _loggerIDs = process.env.LOGGERS || ''
    let _exLoggerIDs = process.env.EXCEPTION_LOGGERS || ''
    let _brokerConnStr = process.env.BROKER || 'amqp://guest:guest@127.0.0.1/'

    // consolidated queue names for one loop initialization
    let _qGroups = {
      loggers: [QN_LOGS],
      exceptionLoggers: [QN_EXCEPTIONS],
      common: [QN_PLUGIN_ID, QN_DEVICES]
    }

    // env clean up, avoiding disclosure
    process.env.CONFIG = undefined
    process.env.BROKER = undefined
    process.env.ACCOUNT = undefined
    process.env.LOGGERS = undefined
    process.env.PLUGIN_ID = undefined
    process.env.EXCEPTION_LOGGERS = undefined

    // adding custom queues in consolidated queue names '_qGroups.*'
    _qGroups.exceptionLoggers = _qGroups.exceptionLoggers.concat(_exLoggerIDs.split(','))
    _qGroups.loggers = _qGroups.loggers.concat(_loggerIDs.split(','))

    // removing empty elements in any (guard)
    _qGroups.exceptionLoggers = _qGroups.exceptionLoggers.filter(Boolean)
    _qGroups.loggers = _qGroups.loggers.filter(Boolean)

    async.series([
      // parse config trap error
      (done) => {
        async.waterfall([
          async.constant(_config.toString('utf8')),
          async.asyncify(JSON.parse)
        ], (err, parsed) => {
          this.config = parsed
          done(err)
        })
      },

      // connecting to rabbitMQ
      (done) => {
        _broker.connect(_brokerConnStr).then(() => {
          return done()
        }).catch(done)
      },

      // setting up generic queues
      (done) => {
        let queueIDs = []

        queueIDs = queueIDs.concat(_qGroups.common)
        queueIDs = queueIDs.concat(_qGroups.loggers)
        queueIDs = queueIDs.concat(_qGroups.exceptionLoggers)

        async.each(queueIDs, (loggerId, cb) => {
          if (isEmpty(loggerId)) return cb()

          _broker.createQueue(loggerId).then(() => {
            return cb()
          }).catch((err) => {
            console.error('DeviceSync newQueue() ', err)
            cb(err)
          })
        }, done)
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
                this.emit('sync')
              } else if (task.operation === 'adddevice') {
                this.emit('adddevice', task.device)
              } else if (task.operation === 'updatedevice') {
                this.emit('updatedevice', task.device)
              } else if (task.operation === 'removedevice') {
                this.emit('removedevice', task.device)
              }
            })
          }
        }

        _broker.queues[QN_PLUGIN_ID].consume(processTask).then(() => {
          return done()
        }).catch(done)
      }

    ], (err) => {
      if (err) {
        console.error('DeviceSync:', err)
        throw err
      }

      process.nextTick(() => {
        // plugin initialized
        this.emit('ready')
      })
    })

    this.syncDevice = (deviceInfo) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(deviceInfo)) {
          return reject(new Error('Kindly specify the device information/details'))
        }

        if (!isPlainObject(deviceInfo)) {
          return reject(new Error('Device info must be an object'))
        }

        if (!(hasProp(deviceInfo, '_id') || hasProp(deviceInfo, 'id'))) {
          return reject(new Error('Kindly specify a valid id for the device'))
        }

        if (!hasProp(deviceInfo, 'name')) {
          return reject(new Error('Kindly specify a valid name for the device'))
        }

        _broker.queues[QN_DEVICES].publish({
          operation: 'sync',
          data: deviceInfo,
          account: ENV_ACCOUNT
        }).then(() => {
          resolve()
        }).catch(reject)

      })
    }

    this.removeDevice = (deviceId) => {
      return new BPromise((resolve, reject) => {
        if (!deviceId) {
          return reject(new Error('Kindly specify the device identifier'))
        }

        if (!isString(deviceId)) {
          return reject(new Error('Device identifier must be a string'))
        }

        _broker.queues[QN_DEVICES].publish({
          operation: 'remove',
          data: {
            _id: deviceId
          },
          account: ENV_ACCOUNT
        }).then(() => {
          resolve()
        }).catch(reject)

      })
    }

    this.log = (logData) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(logData)) return reject(new Error('Kindly specify the data to log'))

        let msg = null

        // loggers and custom loggers are in _qGroups.loggers array
        async.each(_qGroups.loggers, (loggerId, callback) => {
          if (!loggerId) return callback()

          msg = (loggerId === QN_LOGS) ? {account: ENV_ACCOUNT, data: logData} : logData

          // publish() has a built in stringify, so objects are safe to feed
          _broker.queues[loggerId].publish(msg).then(() => {
            callback()
          }).catch(callback)

        }, (err) => {
          if (err) return reject(err)

          resolve()
        })
      })
    }

    this.logException = (err) => {
      return new BPromise((resolve, reject) => {
        if (!isError(err)) return reject(new Error('Kindly specify a valid error to log'))

        let msg = null

        let errData = {
          name: err.name,
          message: err.message,
          stack: err.stack
        }

        async.each(_qGroups.exceptionLoggers, (loggerId, callback) => {
          if (!loggerId) return callback()

          msg = (loggerId === QN_EXCEPTIONS) ? {account: ENV_ACCOUNT, data: errData} : errData

          _broker.queues[loggerId].publish(msg).then(() => {
            callback()
          }).catch(callback)

        }, (err) => {
          if (err) return reject(err)

          resolve()
        })
      })
    }
  }
}

module.exports = DeviceSync
