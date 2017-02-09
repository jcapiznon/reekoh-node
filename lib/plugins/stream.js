'use strict'

const Promise = require('bluebird')
const EventEmitter = require('events').EventEmitter
const async = require('async')
const isEmpty = require('lodash.isempty')
const isPlainObject = require('lodash.isplainobject')
const isNil = require('lodash.isnil')
const Broker = require('../broker.lib')

class Stream extends EventEmitter {
  constructor () {
    super()

    this.outputPipes = process.env.OUTPUT_PIPES.split(',')
    this.loggers = process.env.LOGGERS.split(',')
    this.exceptionLoggers = process.env.EXCEPTION_LOGGERS.split(',')

    this.queues = []
    this.exchange = process.env.PIPELINE
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
        // initialise RPC
        broker.newRpc('client', 'deviceinfo')
          .then((queue) => {
            this.queues['deviceinfo'] = queue
            console.log('RPC initialised')
            done()
          })
          .catch((error) => {
            done(error)
          })
      },
      (done) => {
        let queueIds = this.outputPipes
          .concat(this.loggers)
          .concat(this.exceptionLoggers)
          .concat(['devices', 'data', process.env.PLUGIN_ID])
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
        //  create exchange queue
        broker.newExchangeQueue(this.exchange, 'topic')
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
        this.queues[process.env.PLUGIN_ID].consume((msg) => {
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
  }

  pipe (data, sequenceId) {
    return new Promise((resolve, reject) => {
      if (isEmpty(data) || !isPlainObject(data)) return reject(new Error('Invalid data received. Data should be and Object and not empty.'))

      if (isEmpty(sequenceId)) {
        async.each(this.outputPipes, (outputPipe, done) => {
          this.queues[outputPipe].publish(data)
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
        this.queues['data'].publish({
          plugin: process.env.PLUGIN_ID,
          pipe: process.env.OUTPUT_PIPES,
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

  notifyConnection (deviceId) {
    return new Promise((resolve, reject) => {
      if (isEmpty(deviceId)) return reject(new Error('Please specify the device identifier.'))

      this.queues['devices'].publish({
        data: {
          operation: 'connect',
          device: {
            _id: deviceId
          }
        },
        account: process.env.ACCOUNT
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

  notifyDisconnection (deviceId) {
    return new Promise((resolve, reject) => {
      if (isEmpty(deviceId)) return reject(new Error('Please specify the device identifier.'))

      this.queues['devices'].publish({
        data: {
          operation: 'disconnect',
          device: {
            _id: deviceId
          }
        },
        account: process.env.ACCOUNT
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

  setDeviceState (deviceId, state) {
    return new Promise((resolve, reject) => {
      if (isEmpty(deviceId)) return reject(new Error('Please specify the device identifier.'))

      if (isEmpty(state) || isNil(state)) return reject(new Error('Please specify the device state.'))

      this.queues['devices'].publish({
        data: {
          operation: 'setstate',
          device: {
            _id: deviceId,
            state: state
          }
        },
        account: process.env.ACCOUNT
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

  requestDeviceInfo (deviceId) {
    return new Promise((resolve, reject) => {
      if (isEmpty(deviceId)) return reject(new Error('Please specify the device identifier.'))

      this.queues['deviceinfo'].publish({
          data: {
            device: {
              _id: deviceId
            }
          },
          account: process.env.ACCOUNT
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

module.exports = Stream
