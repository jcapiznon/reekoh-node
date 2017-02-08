'use strict'

const Promise = require('bluebird')
const EventEmitter = require('events').EventEmitter
const async = require('async')
const isEmpty = require('lodash.isempty')
const isPlainObject = require('lodash.isplainobject')
const isNil = require('lodash.isnil')
const Broker = require('../broker.lib')

let outputPipes = []
let loggers = []
let exceptionLoggers = []

class Stream extends EventEmitter {
  constructor () {
    super()

    outputPipes = process.env.OUTPUT_PIPES.split(',')
    loggers = process.env.LOGGERS.split(',')
    exceptionLoggers = process.env.EXCEPTION_LOGGERS.split(',')

    this.queues = []
    this.exchange = process.env.PIPELINE
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
        // initialise RPC
        broker.newRpc('client', 'agent.deviceinfo')
          .then((queue) => {
            this.queues['agent.deviceinfo'] = queue
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
          .concat(['agent.devices', 'agent.data', process.env.PLUGIN_ID])

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
        async.each(outputPipes, (outputPipe, done) => {
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
        this.queues['agent.data'].publish(JSON.stringify({
          plugin: process.env.PLUGIN_ID,
          pipe: process.env.OUTPUT_PIPES,
          sequenceId: sequenceId,
          data: data
        }))
          .then(() => {
            console.log('data written to queue agent.data')
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

      this.queues['agent.devices'].publish(JSON.stringify({
        operation: 'connect',
        device: {
          _id: deviceId
        }
      }))
        .then(() => {
          console.log('data written to queue agent.devices')
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

      this.queues['agent.devices'].publish(JSON.stringify({
        operation: 'disconnect',
        device: {
          _id: deviceId
        }
      }))
        .then(() => {
          console.log('data written to queue agent.devices')
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

      this.queues['agent.devices'].publish(JSON.stringify({
        operation: 'setstate',
        device: {
          _id: deviceId,
          state: state
        }
      }))
        .then(() => {
          console.log('data written to queue agent.devices')
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

      this.queues['agent.deviceinfo'].publish(JSON.stringify({device: {_id: deviceId}}))
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

module.exports = Stream
