'use strict'

const async = require('async')
const BPromise = require('bluebird')
const EventEmitter = require('events').EventEmitter

const isError = require('lodash.iserror')
const isEmpty = require('lodash.isempty')
const isString = require('lodash.isstring')
const isPlainObject = require('lodash.isplainobject')

const Broker = require('../broker.lib')

class Storage extends EventEmitter {

  constructor () {
    super()
    this.config = {}

    const BROKER = process.env.BROKER
    const ACCOUNT = process.env.ACCOUNT
    const INPUT_PIPE = process.env.INPUT_PIPE

    const LOGGERS = `${process.env.LOGGERS || ''}`.split(',').filter(Boolean)
    const EXCEPTION_LOGGERS = `${process.env.EXCEPTION_LOGGERS || ''}`.split(',').filter(Boolean)

    let _broker = new Broker()

    process.env.BROKER = undefined
    process.env.ACCOUNT = undefined
    process.env.LOGGERS = undefined
    process.env.INPUT_PIPE = undefined
    process.env.EXCEPTION_LOGGERS = undefined

    async.series([
      // connecting to rabbitMQ
      (done) => {
        _broker.connect(BROKER).then(() => {
          return done() || null // promise warning fix
        }).catch(done)
      },

      // parse config trap error
      (done) => {
        async.waterfall([
          async.constant(process.env.CONFIG || '{}'),
          async.asyncify(JSON.parse)
        ], (err, parsed) => {
          if (!err) {
            this.config = parsed
            process.env.CONFIG = undefined
          }
          done(err)
        })
      },

      // setting up needed queues
      (done) => {
        let queueIDs = [INPUT_PIPE, 'logs', 'exceptions']
          .concat(EXCEPTION_LOGGERS)
          .concat(LOGGERS)

        async.each(queueIDs, (loggerId, cb) => {
          if (isEmpty(loggerId)) return cb()

          _broker.createQueue(loggerId).then(() => {
            return cb() || null // promise warning fix
          }).catch(cb)
        }, done)
      },

      // process/listen for queued items
      (done) => {
        _broker.queues[INPUT_PIPE].consume((msg) => {
          if (!isEmpty(msg)) {
            async.waterfall([
              async.constant(msg.content.toString('utf8')),
              async.asyncify(JSON.parse)
            ], (err, parsed) => {
              if (!err) return this.emit('data', parsed)
              console.error('Storage processQueue() rcvd data is not a JSON.', err)
            })
          }
        }).then(() => {
          return done()
        }).catch(done)
      }
    ], (err) => {
      if (err) {
        console.error('Storage:', err)
        throw err
      }

      process.nextTick(() => {
        // plugin initialized
        this.emit('ready')
      })
    })

    this.log = (logData) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(logData)) return reject(new Error(`Please specify a data to log.`))

        if (!isPlainObject(logData) && !isString(logData)) return reject(new Error('Log data must be a string or object'))

        async.parallel([
          (callback) => {
            async.each(LOGGERS, (logger, done) => {
              _broker.queues[logger].publish(logData).then(() => {
                done()
              }).catch(done)
            }, callback)
          },
          (callback) => {
            _broker.queues['logs'].publish({
              account: ACCOUNT,
              data: logData
            }).then(() => {
              callback()
            }).catch(callback)
          }
        ], (err) => {
          if (err) return reject(err)
          resolve()
        })
      })
    }

    this.logException = (err) => {
      return new BPromise((resolve, reject) => {
        if (!isError(err)) return reject(new Error('Please specify a valid error to log.'))

        let errData = {
          name: err.name,
          message: err.message,
          stack: err.stack
        }

        async.parallel([
          (callback) => {
            async.each(EXCEPTION_LOGGERS, (logger, done) => {
              _broker.queues[logger].publish(errData).then(() => {
                done()
              }).catch(done)
            }, callback)
          },
          (callback) => {
            _broker.queues['exceptions'].publish({
              account: ACCOUNT,
              data: errData
            }).then(() => {
              callback()
            }).catch(callback)
          }
        ], (err) => {
          if (err) return reject(err)
          resolve()
        })
      })
    }
  }
}

module.exports = Storage
