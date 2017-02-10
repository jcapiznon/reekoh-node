'use strict'

const Promise = require('bluebird')
const EventEmitter = require('events').EventEmitter
const async = require('async')
const isEmpty = require('lodash.isempty')
const Broker = require('../broker.lib')
const isError = require('lodash.iserror')
const isString = require('lodash.isstring')
const isPlainObject = require('lodash.isplainobject')

class Service extends EventEmitter {
  constructor () {
    super()

    let inputPipe = process.env.INPUT_PIPE
    let outputPipes = `${process.env.OUTPUT_PIPES || ''}`.split(',').filter(Boolean)
    let loggers = `${process.env.LOGGERS || ''}`.split(',').filter(Boolean)
    let exceptionLoggers = `${process.env.EXCEPTION_LOGGERS || ''}`.split(',').filter(Boolean)
    let account = process.env.ACCOUNT
    let brokerUrl = process.env.BROKER
    let outputScheme = process.env.OUTPUT_SCHEME
    let outputNamespace = process.env.OUTPUT_NAMESPACE
    let queues = []
    let broker = new Broker()

    process.env.INPUT_PIPE = undefined
    process.env.OUTPUT_PIPES = undefined
    process.env.LOGGERS = undefined
    process.env.EXCEPTION_LOGGERS = undefined
    process.env.ACCOUNT = undefined
    process.env.BROKER = undefined
    process.env.OUTPUT_SCHEME = undefined
    process.env.OUTPUT_NAMESPACE = undefined

    async.waterfall([
      (done) => {
        async.waterfall([
          async.constant(process.env.CONFIG),
          async.asyncify(JSON.parse)
        ], (err, parsed) => {
          if (!err){
            this.config = parsed
            process.env.CONFIG = undefined
          }
          done(err)
        })
      },
      (done) => {
        // connect to rabbitmq
        broker.connect(brokerUrl)
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
          .concat(outputPipes)
          .concat(loggers)
          .concat(exceptionLoggers)
          .concat(['logs', 'exceptions'])

        async.each(queueIds, (queueID, callback) => {
          broker.newQueue(queueID)
            .then((queue) => {
              queues[queueID] = queue
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
        queues[inputPipe].consume((msg) => {
          async.waterfall([
            async.constant(msg.content.toString('utf8')),
            async.asyncify(JSON.parse)
          ], (err, parsed) => {
            if (err) return console.error(err)

            this.emit('data', parsed)
          })
        })
          .then(() => {
            console.log('Input pipes consumed.')
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

        async.waterfall([
          (callback) => {
            async.each(loggers, (logger, done) => {
              queues[logger].publish(logData)
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
              account: account,
              data: logData
            }

            queues['logs'].publish(logData)
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

    this.logException = (err) => {
      return new Promise((resolve, reject) => {
        if (!isError(err)) return reject(new Error('Please specify a valid error to log.'))

        let errData = {
          name: err.name,
          message: err.message,
          stack: err.stack
        }

        async.waterfall([
          (callback) => {
            async.each(exceptionLoggers, (logger, done) => {
              queues[logger].publish(errData)
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
              account: account,
              data: errData
            }

            queues['exceptions'].publish(errData)
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

    this.pipe = (data, result) => {
      return new Promise((resolve, reject) => {
        if (isEmpty(data) || isEmpty(result)) return reject(new Error('Please specify the original data and the result.'))

        if (!isPlainObject(data)) return reject(new Error('Data should be a valid JSON object.'))

        if (outputScheme === 'MERGE') {
          let outputData = Object.assign(data, result)

          async.each(outputPipes, (outputPipe, done) => {
            queues[outputPipe].publish(outputData)
              .then(() => {
                console.log(`message written to queue ${outputPipe}`)
                done()
              })
              .catch((error) => {
                done(error)
              })
          }, (error) => {
            if (error) return reject(error)
            resolve()
          })
        } else if (outputScheme === 'NAMESPACE') {
          let outputData = Object.assign(data[outputNamespace], result)

          async.each(outputPipes, (outputPipe, done) => {
            queues[outputPipe].publish(outputData)
              .then(() => {
                console.log(`message written to queue ${outputPipe}`)
                done()
              })
              .catch((error) => {
                done(error)
              })
          }, (error) => {
            if (error) return reject(error)
            resolve()
          })
        } else if (outputScheme === 'RESULT') {
          async.each(outputPipes, (outputPipe, done) => {
            queues[outputPipe].publish(result)
              .then(() => {
                console.log(`message written to queue ${outputPipe}`)
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
  }
}

module.exports = Service
