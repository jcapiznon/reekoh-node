'use strict'

const async = require('async')
const BPromise = require('bluebird')
const EventEmitter = require('events').EventEmitter

const isError = require('lodash.iserror')
const isEmpty = require('lodash.isempty')
const isString = require('lodash.isstring')
const isPlainObject = require('lodash.isplainobject')

const Broker = require('../broker.lib')

class ExceptionLogger extends EventEmitter {
  constructor () {
    super()

    const BROKER = process.env.BROKER
    const ACCOUNT = process.env.ACCOUNT
    const INPUT_PIPE = process.env.INPUT_PIPE

    let _broker = new Broker()

    process.env.INPUT_PIPE = undefined
    process.env.ACCOUNT = undefined
    process.env.BROKER = undefined

    async.series([
      (done) => {
        async.waterfall([
          async.constant(process.env.CONFIG),
          async.asyncify(JSON.parse)
        ], (err, parsed) => {
          done(err)
          
          if (!err) {
            this.config = parsed
            process.env.CONFIG = undefined
          }
        })
      },
      (done) => {
        // connect to rabbitmq
        _broker.connect(BROKER).then(() => {
          console.log('Connected to RabbitMQ Server.')
          done()
        }).catch(done)
      },
      (done) => {
        let queueIds = [INPUT_PIPE, 'logs', 'exceptions']

        async.each(queueIds, (queueID, cb) => {
          _broker.createQueue(queueID).then(() => {
            cb()
          }).catch(cb)
        }, (err) => {
          if (!err) console.log('Connected to queues.')
          done(err)
        })
      },
      (done) => {
        _broker.queues[INPUT_PIPE].consume((msg) => {
          async.waterfall([
            async.constant(msg.content.toString('utf8')),
            async.asyncify(JSON.parse)
          ], (err, parsed) => {
            if (err) return console.error(err)
            this.emit('exception', parsed)
          })
        }).then(() => {
          done()
        }).catch(done)
      }
    ], (err) => {
      if (err) {
        console.error(err)
        throw err
      }

      process.nextTick(() => {
        console.log('Plugin init process done.')
        this.emit('ready')
      })
    })

    this.log = (logData) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(logData)) return reject(new Error(`Please specify a data to log.`))

        if (!isPlainObject(logData) && !isString(logData)) return reject(new Error('Log data must be a string or object'))

        logData = {
          account: ACCOUNT,
          data: logData
        }

        _broker.queues['logs'].publish(logData).then(() => {
          console.log(`message written to queue logs`)
          resolve()
        }).catch(reject)
      })
    }

    this.logException = (err) => {
      return new BPromise((resolve, reject) => {
        if (!isError(err)) return reject(new Error('Please specify a valid error to log.'))

        let errData = {
          account: ACCOUNT,
          data: {
            name: err.name,
            message: err.message,
            stack: err.stack
          }
        }

        _broker.queues['exceptions'].publish(errData).then(() => {
          console.log(`message written to queue exceptions`)
          resolve()
        }).catch(reject)
      })
    }
  }

}

module.exports = ExceptionLogger
