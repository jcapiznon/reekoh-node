'use strict'

const async = require('async')
const isEmpty = require('lodash.isempty')

const Broker = require('../lib/broker.lib.js')
const EventEmitter = require('events').EventEmitter

class Storage extends EventEmitter {

  constructor () {
    super()

    this.config = {}
    this.queues = []

    this.QN_INPUT_PIPE = process.env.INPUT_PIPE || 'demo.storage'

    this.qn = {
      common: [
        this.QN_INPUT_PIPE
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

    // removing empty elements
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
              if (!err) return _self.emit('data', parsed)
              console.error('Storage processQueue() rcvd data is not a JSON.', err)
            })
          }
        }

        let queueName = _self.QN_INPUT_PIPE
        _self.queues[queueName].consume(processTask)
          .then((msg) => {
            return done() || null
          }).catch((err) => {
            done(err)
          })
      }

    ], (err) => {
      if (err) return console.error('Storage: ', err)

      // plugin initialized
      _self.emit('ready')
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

module.exports = Storage
