'use strict'

let Promise = require('bluebird')
let amqplib = require('amqplib')
let EventEmitter = require('events').EventEmitter
let async = require('async')
let isEmpty = require('lodash.isempty')
let loggers = process.env.LOGGERS.split(',')
let exceptionLoggers = process.env.EXCEPTION_LOGGERS.split(',')
let Broker = require('../lib/messenger.lib.js')
let isPlainObject = require('lodash.isplainobject')
let isString = require('lodash.isstring')

class Connector extends EventEmitter {
  constructor () {
    console.log('constructor')
    super()
    this.config = JSON.parse(process.env.CONFIG)
    this.INPUT_PIPE = process.env.INPUT_PIPE
    this._broker = new Broker()

    let broker = this._broker

    loggers.push('generic.logs')
    exceptionLoggers.push('generic.exceptions')

    async.waterfall([
      (done) => {
        // connect to rabbitmq
        broker.connect(process.env.BROKER)
          .then(() => {
            console.log('Connected to RabbitMQ Server.')
            done()
          })
          .catch((error) => {
            console.error('Could not connect to RabbitMQ Server. ERR:', error)
          })
      },
      (done) => {
        // connect to loggers queue
        async.each(loggers, (logger, eCallback) => {
          broker.assert(logger)
            .then(() => {
              console.log(`Connected to ${logger}`)
              eCallback()
            })
            .catch((error) => {
              eCallback(error)
            })
        }, (error) => {
          done(error)
        })
      },
      (done) => {
        // connect to exception loggers queue
        async.each(exceptionLoggers, (logger, eCallback) => {
          broker.assert(logger)
            .then(() => {
              console.log(`Connected to ${logger}`)
              eCallback()
            })
            .catch((error) => {
              eCallback(error)
            })
        }, (error) => {
          done(error)
        })
      }
    ], (error) => {
      if (error) return console.error(error)
    })
  }

  log (logData) {
    console.log('log')
    return new Promise((resolve, reject) => {
      if (isEmpty(logData)) return reject(new Error(`Please specify a data to log.`))

      if (isPlainObject(logData)) {
        // send to queue
        async.each(loggers, (logger, done) => {
          this._broker.send(logger, new Buffer(JSON.stringify(logData)))
            .then(() => {
              console.log('message written to queue')
              done()
            })
            .catch((error) => {
              done(error)
            })
        }, (error) => {
          if (error) return reject(error)
          resolve
        })
      }

      if (isString(logData)) {
        //  send to queue
        async.each(loggers, (logger, done) => {
          this._broker.send(logger, new Buffer(logData))
            .then(() => {
              console.log(`message written to queue ${logger}`)
              done()
            })
            .catch((error) => {
              done(error)
            })
        }, (error) => {
          if (error) return reject(error)
          resolve()
        })
      }
    })
  }

  logException (err) {
    return new Promise((resolve, reject) => {
      async.each(exceptionLoggers, (logger, done) => {
        this._broker.send(logger, new Buffer(JSON.stringify({
          name: err.name,
          message: err.message,
          stack: err.stack
        })))
          .then(() => {
            console.log(`message written to queue ${logger}`)
            done()
          })
          .catch((error) => {
            done(error)
          })
      }, (error) => {
        if (error) return reject(error)
        resolve()
      })
    })
  }
}

module.exports = Connector
