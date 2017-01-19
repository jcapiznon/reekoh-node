'use strict'

let Promise = require('bluebird')
let EventEmitter = require('events').EventEmitter
let async = require('async')
let isEmpty = require('lodash.isempty')
let Broker = require('../lib/broker.lib')
let inputPipe = null
let loggers = []
let exceptionLoggers = []

class Connector extends EventEmitter {
  constructor () {
    super()

    inputPipe = process.env.INPUT_PIPE
    loggers = process.env.LOGGERS.split(',')
    exceptionLoggers = process.env.EXCEPTION_LOGGERS.split(',')

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

    loggers.push('agent.logs')
    exceptionLoggers.push('agent.exceptions')

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
        let queueIds = [inputPipe]
          .concat(loggers)
          .concat(exceptionLoggers)

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
        this.queues[inputPipe].consume((msg) => {
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

      async.each(loggers, (logger, done) => {
        this.queues[logger].publish(logData)
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

  logException (err) {
    let errData = {
      name: err.name,
      message: err.message,
      stack: err.stack
    }

    return new Promise((resolve, reject) => {
      async.each(exceptionLoggers, (logger, done) => {
        this.queues[logger].publish(errData)
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
