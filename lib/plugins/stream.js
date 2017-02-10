'use strict'

const Promise = require('bluebird')
const EventEmitter = require('events').EventEmitter
const async = require('async')
const isEmpty = require('lodash.isempty')
const isPlainObject = require('lodash.isplainobject')
const isNil = require('lodash.isnil')
const Broker = require('../broker.lib')
const isError = require('lodash.iserror')
const isString = require('lodash.isstring')

class Stream extends EventEmitter {
  constructor () {
    super()

    let outputPipes = `${process.env.OUTPUT_PIPES || ''}`.split(',').filter(Boolean)
    let envOutputPipes = process.env.OUTPUT_PIPES
    let loggers = `${process.env.LOGGERS || ''}`.split(',').filter(Boolean)
    let exceptionLoggers = `${process.env.EXCEPTION_LOGGERS || ''}`.split(',').filter(Boolean)
    let account = process.env.ACCOUNT
    let pluginId = process.env.PLUGIN_ID
    let queues = []
    let exchange = process.env.PIPELINE
    let broker = new Broker()
    let brokerUrl = process.env.BROKER

    process.env.ACCOUNT = undefined
    process.env.PLUGIN_ID = undefined
    process.env.PIPELINE = undefined
    process.env.OUTPUT_PIPES = undefined
    process.env.LOGGERS = undefined
    process.env.EXCEPTION_LOGGERS = undefined
    process.env.BROKER = undefined

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
        // initialise RPC
        broker.newRpc('client', 'deviceinfo')
          .then((queue) => {
            queues['deviceinfo'] = queue
            console.log('RPC initialised')
            done()
          })
          .catch((error) => {
            done(error)
          })
      },
      (done) => {
        let queueIds = outputPipes
          .concat(loggers)
          .concat(exceptionLoggers)
          .concat(['devices', 'data', pluginId])
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
        //  create exchange queue
        broker.newExchangeQueue(exchange, 'topic')
          .then((queue) => {
            console.log('Exchange created.')
            queue.consume((msg) => {
              async.waterfall([
                async.constant(msg.content.toString('utf8')),
                async.asyncify(JSON.parse)
              ], (err, parsed) => {
                if (err) return console.error(err)

                this.emit('message', parsed)
              })
            })
            done()
          })
          .catch((error) => {
            done(error)
          })
      },
      (done) => {
        queues[pluginId].consume((msg) => {
          async.waterfall([
            async.constant(msg.content.toString('utf8')),
            async.asyncify(JSON.parse)
          ], (err, parsed) => {
            if (err) return console.error(err)

            this.emit('sync')
          })
        })

        done()
      }
    ], (error) => {
      if (error) return console.error(error)

      console.log('Plugin init process done.')
      this.emit('ready')
    })

    this.pipe = (data, sequenceId) => {
      return new Promise((resolve, reject) => {
        if (isEmpty(data) || !isPlainObject(data)) return reject(new Error('Invalid data received. Data should be and Object and not empty.'))

        if (!isPlainObject(data)) return reject(new Error('Data should be a valid JSON object.'))

        if (isEmpty(sequenceId)) {
          async.each(outputPipes, (outputPipe, done) => {
            queues[outputPipe].publish(data)
              .then(() => {
                console.log(`data written to queue ${outputPipe}`)
                done()
              })
              .catch((error) => {
                done(error)
              })
          }, (error) => {
            if (error) return reject(error)

            resolve()
          })
        } else {
          if (!isString(sequenceId)) return reject(new Error('SequenceID should be a string.'))

          queues['data'].publish({
            plugin: pluginId,
            pipe: envOutputPipes,
            sequenceId: sequenceId,
            data: data
          })
            .then(() => {
              console.log('data written to queue data')
              resolve()
            })
            .catch((error) => {
              reject(error)
            })
        }
      })
    }

    this.notifyConnection = (deviceId) => {
      return new Promise((resolve, reject) => {
        if (isEmpty(deviceId)) return reject(new Error('Please specify the device identifier.'))

        if (!isString(deviceId)) return reject(new Error('DeviceID should be a string.'))

        queues['devices'].publish({
          data: {
            operation: 'connect',
            device: {
              _id: deviceId
            }
          },
          account: account
        })
          .then(() => {
            console.log('data written to queue devices')
            resolve()
          })
          .catch((error) => {
            reject(error)
          })
      })
    }

    this.notifyDisconnection = (deviceId) => {
      return new Promise((resolve, reject) => {
        if (isEmpty(deviceId)) return reject(new Error('Please specify the device identifier.'))

        if (!isString(deviceId)) return reject(new Error('DeviceID should be a string.'))

        queues['devices'].publish({
          data: {
            operation: 'disconnect',
            device: {
              _id: deviceId
            }
          },
          account: account
        })
          .then(() => {
            console.log('data written to queue devices')
            resolve()
          })
          .catch((error) => {
            reject(error)
          })
      })
    }

    this.setDeviceState = (deviceId, state) => {
      return new Promise((resolve, reject) => {
        if (isEmpty(deviceId)) return reject(new Error('Please specify the device identifier.'))

        if (!isString(deviceId)) return reject(new Error('DeviceID should be a string.'))

        if (isEmpty(state) || isNil(state)) return reject(new Error('Please specify the device state.'))

        queues['devices'].publish({
          data: {
            operation: 'setstate',
            device: {
              _id: deviceId,
              state: state
            }
          },
          account: account
        })
          .then(() => {
            console.log('data written to queue devices')
            resolve()
          })
          .catch((error) => {
            reject(error)
          })
      })
    }

    this.requestDeviceInfo = (deviceId) => {
      return new Promise((resolve, reject) => {
        if (isEmpty(deviceId)) return reject(new Error('Please specify the device identifier.'))

        if (!isString(deviceId)) return reject(new Error('DeviceID should be a string.'))

        queues['deviceinfo'].publish({
          data: {
            device: {
              _id: deviceId
            }
          },
          account: account
        })
          .then((reply) => {
            console.log('Reply from server has been received.')
            resolve(reply)
          })
          .catch((error) => {
            reject(error)
          })
      })
    }

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

  }
}

module.exports = Stream
