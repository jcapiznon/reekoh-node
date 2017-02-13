'use strict'

const async = require('async')
const BPromise = require('bluebird')
const EventEmitter = require('events').EventEmitter

const isEmpty = require('lodash.isempty')
const isError = require('lodash.iserror')
const isString = require('lodash.isstring')
const isPlainObject = require('lodash.isplainobject')

const Broker = require('../broker.lib')

class Service extends EventEmitter {
  constructor () {
    super()

    const BROKER = process.env.BROKER
    const ACCOUNT = process.env.ACCOUNT
    const INPUT_PIPE = process.env.INPUT_PIPE

    const OUTPUT_PIPES = `${process.env.OUTPUT_PIPES || ''}`.split(',').filter(Boolean)
    const OUTPUT_SCHEME = `${process.env.OUTPUT_SCHEME || 'MERGE'}`.toUpperCase()
    const OUTPUT_NAMESPACE = `${process.env.OUTPUT_NAMESPACE || 'result'}`

    const LOGGERS = `${process.env.LOGGERS || ''}`.split(',').filter(Boolean)
    const EXCEPTION_LOGGERS = `${process.env.EXCEPTION_LOGGERS || ''}`.split(',').filter(Boolean)

    let _broker = new Broker()

    process.env.INPUT_PIPE = undefined
    process.env.OUTPUT_PIPES = undefined
    process.env.LOGGERS = undefined
    process.env.EXCEPTION_LOGGERS = undefined
    process.env.ACCOUNT = undefined
    process.env.BROKER = undefined
    process.env.OUTPUT_SCHEME = undefined
    process.env.OUTPUT_NAMESPACE = undefined

    async.series([
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
        _broker.connect(BROKER).then(() => {
          console.log('Connected to RabbitMQ Server.')
          done()
        }).catch(done)
      },
      (done) => {
        let queueIds = [INPUT_PIPE]
          .concat(OUTPUT_PIPES)
          .concat(LOGGERS)
          .concat(EXCEPTION_LOGGERS)
          .concat(['logs', 'exceptions'])

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
        // consume input pipe
        _broker.queues[INPUT_PIPE].consume((msg) => {
          async.waterfall([
            async.constant(msg.content.toString('utf8')),
            async.asyncify(JSON.parse)
          ], (err, parsed) => {
            if (err) return console.error(err)

            this.emit('data', parsed)
          })
        }).then(() => {
          console.log('Input pipes consumed.')
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

    this.pipe = (data, result) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(data) || isEmpty(result)) return reject(new Error('Please specify the original data and the result.'))

        if (!isPlainObject(data)) return reject(new Error('Data should be a valid JSON object.'))

        let sendResult = (outputData) => {
          async.each(OUTPUT_PIPES, (outputPipe, done) => {
            _broker.queues[outputPipe].publish(outputData)
              .then(() => {
                console.log(`message written to queue ${outputPipe}`)
                done()
              })
              .catch((err) => {
                done(err)
              })
          }, (err) => {
            if (err) return reject(err)
            resolve()
          })
        }

        if (OUTPUT_SCHEME === 'MERGE') {
          sendResult(Object.assign(data, result))
        } else if (OUTPUT_SCHEME === 'NAMESPACE') {
          sendResult(Object.assign(data[OUTPUT_NAMESPACE], result))
        } else if (OUTPUT_SCHEME === 'RESULT') {
          sendResult(result)
        }
      })
    }

    this.log = (logData) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(logData)) return reject(new Error(`Please specify a data to log.`))

        if (!isPlainObject(logData) && !isString(logData)) return reject(new Error('Log data must be a string or object'))

        async.parallel([
          (callback) => {
            async.each(LOGGERS, (logger, done) => {
              _broker.queues[logger].publish(logData).then(() => {
                console.log(`message written to queue ${logger}`)
                done()
              }).catch(done)
            }, callback)
          },
          (callback) => {
            let data = {
              account: ACCOUNT,
              data: logData
            }

            _broker.queues['logs'].publish(data).then(() => {
              console.log(`message written to queue logs`)
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
                console.log(`message written to queue ${logger}`)
                done()
              }).catch(done)
            }, callback)
          },
          (callback) => {
            let data = {
              account: ACCOUNT,
              data: errData
            }

            _broker.queues['exceptions'].publish(data).then(() => {
              console.log(`message written to queue exceptions`)
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

module.exports = Service
