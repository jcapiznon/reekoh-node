'use strict'

let Promise = require('bluebird')
let EventEmitter = require('events').EventEmitter
let async = require('async')
let isEmpty = require('lodash.isempty')
let Broker = require('../lib/messenger.lib.js')
let isPlainObject = require('lodash.isplainobject')
let isString = require('lodash.isstring')
let inputPipes = process.env.INPUT_PIPES.split(',')
let loggers = process.env.LOGGERS.split(',')
let exceptionLoggers = process.env.EXCEPTION_LOGGERS.split(',')

class Connector extends EventEmitter {
  constructor () {
    console.log('constructor')
    super()

    let dataEmitter = (msg) => {
      async.waterfall([
        async.constant(msg.content.toString('utf8')),
        async.asyncify(JSON.parse)
      ], (err, parsed) => {
        if (err) return console.error(err)

        this.emit('data', parsed)
      })
    }

    this._broker = new Broker()
    let broker = this._broker

    loggers.push('generic.logs')
    exceptionLoggers.push('generic.exceptions')

    async.waterfall([
      (done) => {
        async.waterfall([
          async.constant(process.env.CONFIG),
          async.asyncify(JSON.parse)
        ], (err, parsed) => {
          done(err)
          this.config = parsed
        })
      },
      (done) => {
        // connect to rabbitmq
        broker.connect(process.env.BROKER)
          .then(() => {
            console.log('Connected to RabbitMQ Server.')
            done()
          })
          .catch((error) => {
            done(error)
          })
      },
      (done) => {
        //  assert consumer queues
        async.each(inputPipes, (inputPipe, callback) => {
          broker.assert(inputPipe)
            .then(() => {
              console.log(`Connected to ${inputPipe}`)
              callback()
            })
            .catch((error) => {
              callback(error)
            })
        }, (error) => {
          done(error)
        })
      },
      (done) => {
        // consume input pipes
        async.each(inputPipes, (inputPipe, callback) => {
          broker.consume(inputPipe, (msg) => {
            dataEmitter(msg)
          })
            .then(() => {
              callback()
            })
            .catch((error) => {
              callback(error)
            })
        }, (error) => {
          done(error)
        })
      },
      (done) => {
        // connect to loggers queue
        async.each(loggers, (logger, callback) => {
          broker.assert(logger)
            .then(() => {
              console.log(`Connected to ${logger}`)
              callback()
            })
            .catch((error) => {
              callback(error)
            })
        }, (error) => {
          done(error)
        })
      },
      (done) => {
        // connect to exception loggers queue
        async.each(exceptionLoggers, (logger, callback) => {
          broker.assert(logger)
            .then(() => {
              console.log(`Connected to ${logger}`)
              callback()
            })
            .catch((error) => {
              callback(error)
            })
        }, (error) => {
          done(error)
        })
      }
    ], (error) => {
      if (error) return console.error(error)

      this.emit('ready')
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
          resolve()
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
