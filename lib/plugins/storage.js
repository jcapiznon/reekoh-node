'use strict'

const async = require('async')
const isError = require('lodash.iserror')
const isEmpty = require('lodash.isempty')

const Broker = require('../broker.lib')
const EventEmitter = require('events').EventEmitter

class Storage extends EventEmitter {

  constructor () {
    super()
    this.config = {}

    const ENV_ACCOUNT = process.env.ACCOUNT || ''

    const QN_LOGS = 'logs'
    const QN_EXCEPTIONS = 'exceptions'
    const QN_INPUT_PIPE = process.env.INPUT_PIPE || 'demo.storage'

    let _queues = []
    let _broker = new Broker()

    let _config = process.env.CONFIG || '{}'
    let _loggerIDs = process.env.LOGGERS || ''
    let _exLoggerIDs = process.env.EXCEPTION_LOGGERS || ''
    let _brokerConnStr = process.env.BROKER || 'amqp://guest:guest@127.0.0.1/'

    // consolidated queue names for one loop initialization
    let _qGroups = {
      loggers: [QN_LOGS],
      common: [QN_INPUT_PIPE],
      exceptionLoggers: [QN_EXCEPTIONS]
    }

    // env clean up, avoiding disclosure
    process.env.BROKER = undefined
    process.env.CONFIG = undefined
    process.env.LOGGERS = undefined
    process.env.ACCOUNT = undefined
    process.env.INPUT_PIPE = undefined
    process.env.EXCEPTION_LOGGERS = undefined

    // adding custom queues in consolidated queue names '_qGroups.*'
    _qGroups.loggers = _qGroups.loggers.concat(_loggerIDs.split(','))
    _qGroups.exceptionLoggers = _qGroups.exceptionLoggers.concat(_exLoggerIDs.split(','))

    // removing empty elements, if any
    _qGroups.loggers = _qGroups.loggers.filter(Boolean)
    _qGroups.exceptionLoggers = _qGroups.exceptionLoggers.filter(Boolean)

    async.waterfall([

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
        _broker.connect(_brokerConnStr)
          .then(() => {
            return done() || null // !
          }).catch((err) => {
            done(err)
          })
      },

      // setting up needed queues
      (done) => {
        let queueIDs = []

        queueIDs = queueIDs.concat(_qGroups.common)
        queueIDs = queueIDs.concat(_qGroups.loggers)
        queueIDs = queueIDs.concat(_qGroups.exceptionLoggers)

        async.each(queueIDs, (loggerId, callback) => {
          if (isEmpty(loggerId)) return callback()

          _broker.newQueue(loggerId)
            .then((queue) => {
              if (queue) _queues[loggerId] = queue
              return callback() || null // !
            }).catch((err) => {
              console.error('Storage newQueue()', err)
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
            ], (err, parsed) => {
              if (!err) return this.emit('data', parsed)
              console.error('Storage processQueue() rcvd data is not a JSON.', err)
            })
          }
        }

        _queues[QN_INPUT_PIPE].consume(processTask)
          .then((msg) => {
            return done() || null
          }).catch((err) => {
            done(err)
          })
      }

    ], (err) => {
      if (err) return console.error('Storage: ', err)

      // plugin initialized
      this.emit('ready')
    })

    this.log = (logData) => {
      let msg = null

      return new Promise((resolve, reject) => {
        if (isEmpty(logData)) return reject(new Error('Kindly specify the data to log'))

        // loggers and custom loggers are in _qGroups.loggers array
        async.each(_qGroups.loggers, (loggerId, callback) => {
          if (!loggerId) return callback()

          msg = loggerId === QN_LOGS
            ? { account: ENV_ACCOUNT, data: logData }
            : logData

          // publish() has a built in stringify, so objects are safe to feed
          _queues[loggerId].publish(msg)
            .then(() => {
              callback()
            }).catch((err) => {
              reject(err)
            })

        }, (err) => {
          if (err) return reject(err)
          resolve()
        })
      })
    }

    this.logException = (err) => {
      let msg = null

      let errData = {
        name: err.name,
        message: err.message,
        stack: err.stack
      }

      return new Promise((resolve, reject) => {
        if (!isError(err)) return reject(new Error('Kindly specify a valid error to log'))

        async.each(_qGroups.exceptionLoggers, (loggerId, callback) => {
          if (!loggerId) return callback()

          msg = loggerId === QN_EXCEPTIONS
            ? { account: ENV_ACCOUNT, data: errData }
            : errData

          _queues[loggerId].publish(msg)
            .then(() => {
              callback()
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
}

module.exports = Storage