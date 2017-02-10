'use strict'

const Promise = require('bluebird')
const EventEmitter = require('events').EventEmitter
const async = require('async')
const isEmpty = require('lodash.isempty')
const Broker = require('../broker.lib')
const isString = require('lodash.isstring')
const isPlainObject = require('lodash.isplainobject')
const isError = require('lodash.iserror')

class ExceptionLogger extends EventEmitter {
  constructor () {
    super()

    let inputPipe = process.env.INPUT_PIPE
    let account = process.env.ACCOUNT
    let brokerEnv = process.env.BROKER
    this._broker = new Broker()
    let _queues = []
    let broker = this._broker

    process.env.INPUT_PIPE = undefined
    process.env.ACCOUNT = undefined
    process.env.BROKER = undefined

    let dataEmitter = (msg) => {
      async.waterfall([
        async.constant(msg.content.toString('utf8')),
        async.asyncify(JSON.parse)
      ], (err, parsed) => {
        if (err) return console.error(err)
        this.emit('exception', parsed)
      })
    }

    async.waterfall([
      (done) => {
        async.waterfall([
          async.constant(process.env.CONFIG),
          async.asyncify(JSON.parse)
        ], (err, parsed) => {
          if (!err) {
            this.config = parsed
            process.env.CONFIG = undefined
          }
          done(err)
        })
      },
      (done) => {
        // connect to rabbitmq
        broker.connect(brokerEnv)
          .then(() => {
            console.log('Connected to RabbitMQ Server.')
            done()
          })
          .catch((error) => {
            done(error)
          })
      },
      (done) => {
        let queueIds = [ inputPipe, 'logs', 'exceptions' ]
        async.each(queueIds, (queueID, callback) => {
          broker.newQueue(queueID)
            .then((queue) => {
              _queues[queueID] = queue
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
        _queues[inputPipe].consume((msg) => {
          dataEmitter(msg)
        })
          .then(() => {
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

    this.log = (logData) => {
      return new Promise((resolve, reject) => {
        if (isEmpty(logData)) return reject(new Error(`Please specify a data to log.`))

        if (!isPlainObject(logData) && !isString(logData)) return reject(new Error('Log data must be a string or object'))

        logData = {
          account: account,
          data: logData
        }

        _queues['logs'].publish(logData)
          .then(() => {
            console.log('message written to logs')
          })
          .catch((error) => {
            console.error(error)
          })
      })
    }
    this.logException = (err) => {
      let errData = {
        name: err.name,
        message: err.message,
        stack: err.stack
      }

      return new Promise((resolve, reject) => {
        if (!isError(err)) return reject(new Error(`Kindly specify a valid error to log.`))

        errData = {
          account: account,
          data: errData
        }

        _queues['exceptions'].publish(errData)
          .then(() => {
            resolve()
          })
          .catch((error) => {
            reject(error)
          })
      })
    }
  }

}

module.exports = ExceptionLogger
