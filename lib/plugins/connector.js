'use strict'

const Promise = require('bluebird')
const EventEmitter = require('events').EventEmitter
const async = require('async')
const isEmpty = require('lodash.isempty')
const Broker = require('../broker.lib')

class Connector extends EventEmitter {
  constructor () {
    super()

    this.inputPipe = process.env.INPUT_PIPE
    this.loggers = process.env.LOGGERS.split(',')
    this.exceptionLoggers = process.env.EXCEPTION_LOGGERS.split(',')

    let dataEmitter = (msg) => {
      async.waterfall([
        async.constant(msg.content.toString('utf8')),
        async.asyncify(JSON.parse)
      ], (err, parsed) => {
        if (err) return console.error(err)

        this.emit('data', parsed)
      })
    }

    this.queues = []
    this._broker = new Broker()
    let broker = this._broker

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
        let queueIds = [this.inputPipe]
          .concat(this.loggers)
          .concat(this.exceptionLoggers)
          .concat(['logs', 'exceptions'])

        async.each(queueIds, (queueID, callback) => {
          broker.newQueue(queueID)
            .then((queue) => {
              this.queues[queueID] = queue
              callback()
            })
            .catch((error) => {
              callback(error)
            })
        }, (error) => {
          if (!error) console.log('Connected to queues.')
          done(error)
        })
      },
      (done) => {
        // consume input pipe
        this.queues[this.inputPipe].consume((msg) => {
          dataEmitter(msg)
        })
          .then(() => {
            console.log('Input pipe consumed.')
            done()
          })
          .catch((error) => {
            done(error)
          })
      }
    ], (error) => {
      if (error) return console.error(error)

      console.log('Plugin init process done.')
      this.emit('ready')
    })
  }

  log (logData) {
    return new Promise((resolve, reject) => {
      if (isEmpty(logData)) return reject(new Error(`Please specify a data to log.`))

      async.waterfall([
        (callback) => {
          async.each(this.loggers, (logger, done) => {
            this.queues[logger].publish(logData)
              .then(() => {
                console.log(`message written to queue ${logger}`)
                done()
              })
              .catch((error) => {
                done(error)
              })
          }, (error) => {
            callback(error)
          })
        },
        (callback) => {
          logData = {
            account: process.env.ACCOUNT,
            data: logData
          }
          
          this.queues['logs'].publish(logData)
            .then(() => {
              console.log(`message written to queue logs`)
              callback()
            })
            .catch((error) => {
              callback(error)
            })
        }
      ], (error) => {
        if (error) return reject(error)
        resolve()
      })
    })
  }

  logException (err) {
    let errData = {
      name: err.name,
      message: err.message,
      stack: err.stack
    }

    return new Promise((resolve, reject) => {
      async.waterfall([
        (callback) => {
          async.each(this.exceptionLoggers, (logger, done) => {
            this.queues[logger].publish(errData)
              .then(() => {
                console.log(`message written to queue ${logger}`)
                done()
              })
              .catch((error) => {
                done(error)
              })
          }, (error) => {
            callback(error)
          })
        },
        (callback) => {
          errData = {
            account: process.env.ACCOUNT,
            data: errData
          }

          this.queues['exceptions'].publish(errData)
            .then(() => {
              console.log(`message written to queue exceptions`)
              callback()
            })
            .catch((error) => {
              callback(error)
            })
        }
      ], (error) => {
        if (error) return reject(error)
        resolve()
      })
    })
  }
}

module.exports = Connector
