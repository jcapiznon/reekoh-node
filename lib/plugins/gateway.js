'use strict'

const fs = require('fs')
const path = require('path')
const async = require('async')
const Promise = require('bluebird')

const isNil = require('lodash.isnil')
const hasProp = require('lodash.has')
const isEmpty = require('lodash.isempty')
const isString = require('lodash.isstring')

const Broker = require('../broker.lib')
const EventEmitter = require('events').EventEmitter

class Gateway extends EventEmitter {

  constructor () {
    super()

    this.config = {}
    this.queues = []

    this.QN_AGENT_DATA = 'agent.data'
    this.QN_AGENT_DEVICES = 'agent.devices'
    this.QN_AGENT_MESSAGES = 'agent.messages'
    this.QN_AGENT_DEVICE_INFO = 'agent.deviceinfo'
    this.QN_PLUGIN_ID = process.env.PLUGIN_ID || 'demo.gateway'
    this.QN_PIPELINE = process.env.PIPELINE || 'demo.pipeline'

    this.qn = {
      common: [
        this.QN_PLUGIN_ID,
        this.QN_AGENT_DATA,
        this.QN_AGENT_DEVICES,
        this.QN_AGENT_MESSAGES
      ],
      loggers: ['agent.logs'],
      exceptionLoggers: ['agent.exceptions']
    }

    let _self = this
    let _broker = new Broker()

    let _config = process.env.CONFIG || '{}'
    let _loggerIDs = process.env.LOGGERS || ''
    let _outputPipes = process.env.OUTPUT_PIPES || ''
    let _exLoggerIDs = process.env.EXCEPTION_LOGGERS || ''
    let _brokerConnStr = process.env.BROKER || 'amqp://guest:guest@127.0.0.1/'

    // preparing logger queue names in array
    _self.qn.exceptionLoggers = _self.qn.exceptionLoggers.concat(_exLoggerIDs.split(','))
    _self.qn.outPipes = _self.qn.loggers.concat(_outputPipes.split(','))
    _self.qn.loggers = _self.qn.loggers.concat(_loggerIDs.split(','))

    // removing empty elements
    _self.qn.exceptionLoggers = _self.qn.exceptionLoggers.filter(Boolean)
    _self.qn.outPipes = _self.qn.outPipes.filter(Boolean)
    _self.qn.loggers = _self.qn.loggers.filter(Boolean)

    _self.port = process.env.PORT || 8080

    async.waterfall([

      // connecting to rabbitMQ
      (done) => {
        _broker.connect(_brokerConnStr)
          .then(() => {
            return done() || null // !
          }).catch((err) => {
            done(err)
          })
      },

      // parse config trap error
      (done) => {
        async.waterfall([
          async.constant(_config.toString('utf8')),
          async.asyncify(JSON.parse)
        ], (err, parsed) => {
          _self.config = parsed
          done(err)
        })
      },

      // prepare certs/keys needed by SDK plugin developer
      (done) => {
        let root = process.cwd()
        let home = path.join(root, 'keys')

        if (!fs.existsSync(home)) fs.mkdirSync(home)

        let writer = (content, filePath, callback) => {
          if (isEmpty(content)) return callback(null, '')
          fs.writeFile(filePath, content, (err) => { callback(err, filePath) })
        }

        async.series({
          ca: (callback) => { writer(process.env.CA, path.join(home, 'server-ca.pem'), callback) },
          crl: (callback) => { writer(process.env.CRL, path.join(home, 'server-crl.pem'), callback) },
          key: (callback) => { writer(process.env.KEY, path.join(home, 'server-key.pem'), callback) },
          cert: (callback) => { writer(process.env.CERT, path.join(home, 'server-cert.pem'), callback) }
        }, (err, res) => {
          Object.assign(_self, res)
          done(err)
        })
      },

      // setting up generic queues
      (done) => {
        let queueIDs = []

        queueIDs = queueIDs.concat(_self.qn.common)
        queueIDs = queueIDs.concat(_self.qn.loggers)
        queueIDs = queueIDs.concat(_self.qn.outPipes)
        queueIDs = queueIDs.concat(_self.qn.exceptionLoggers)

        async.each(queueIDs, (loggerId, callback) => {
          if (isEmpty(loggerId)) return callback()

          _broker.newQueue(loggerId)
            .then((queue) => {
              if (queue) _self.queues[loggerId] = queue
              return callback() || null // !
            }).catch((err) => {
              console.error('Gateway newQueue()', err)
            })
        }, (err) => {
          done(err)
        })
      },

      // initialize topic exhange for messages
      (done) => {
        let queueName = _self.QN_PIPELINE

        let processTopicData = (msg) => {
          if (!isEmpty(msg)) {
            async.waterfall([
              async.constant(msg.content.toString('utf8')),
              async.asyncify(JSON.parse)
            ], (err, parsed) => {
              if (!err) return _self.emit('message', parsed)
              console.error('Gateway processQueue() rcvd data is not a JSON.', err)
            })
          }
        }

        _broker.newExchangeQueue(queueName)
          .then((queue) => {
            if (!queue) throw new Error('newExchangeQueue() fail')
            _self.queues[queueName] = queue
            return new Promise((resolve) => {
              resolve(queue)
            })
          }).then((queue) => {
            return queue.consume(processTopicData)
          }).then(() => {
            return done() || null // !
          }).catch((err) => {
            done(err)
          })
      },

      // setting up RPC queue
      (done) => {
        let queueName = _self.QN_AGENT_DEVICE_INFO
        _broker.newRpc('client', queueName)
          .then((queue) => {
            if (queue) _self.queues[queueName] = queue
            return done() || null
          }).catch((err) => {
            done(err)
          })
      }

      // plugin initialized
    ], (err) => {
      if (err) return console.error('Gateway: ', err)
      _self.emit('ready')
    })
  }

  pipe (data, sequenceId) {
    let self = this

    return new Promise((resolve, reject) => {
      if (!data) {
        return reject(new Error('Kindly specify the data to forward'))
      }

      let packet = JSON.stringify({
        plugin: process.env.PLUGIN_ID,
        pipe: process.env.OUTPUT_PIPES,
        sequenceId: sequenceId,
        data: data
      })

      let sendToPipes = () => {
        async.each(self.qn.outPipes, (pipe, callback) => {
          if (!pipe) return callback()

          self.queues[pipe].publish(packet)
            .then(() => {
              resolve()
            }).catch((err) => {
              reject(err)
            })
        }, (err) => {
          if (err) return reject(err)
          resolve()
        })
      }

      let sendToSanitizer = () => {
        let queueName = this.QN_AGENT_DATA
        let queue = this.queues[queueName]

        queue.publish(packet)
          .then(() => {
            resolve()
          }).catch((err) => {
            reject(err)
          })
      }

      if (!sequenceId) {
        sendToPipes()
      } else {
        sendToSanitizer()
      }
    })
  }

  relayMessage (message, deviceTypes, devices) {
    let queueName = this.QN_AGENT_MESSAGES
    let queue = this.queues[queueName]

    return new Promise((resolve, reject) => {
      if (!message) {
        return reject(new Error('Kindly specify the command/message to send'))
      }
      if (isEmpty(devices) && isEmpty(deviceTypes)) {
        return reject(new Error('Kindly specify the target device types or devices'))
      }

      if ((isString(devices) || Array.isArray(devices)) &&
        (isString(deviceTypes) || Array.isArray(deviceTypes))) {
        let data = JSON.stringify({
          pipeline: process.env.PIPELINE,
          message: message,
          deviceTypes: deviceTypes,
          devices: devices
        })

        queue.publish(data)
          .then(() => {
            resolve()
          }).catch((err) => {
            reject(err)
          })
      } else {
        return reject(new Error("'devices' and 'deviceTypes' must be a string or an array."))
      }
    })
  }

  notifyConnection (deviceId) {
    let queueName = this.QN_AGENT_DEVICES
    let queue = this.queues[queueName]

    return new Promise((resolve, reject) => {
      if (!deviceId) { // string can be falsy
        return reject(new Error('Kindly specify the device identifier'))
      }

      let data = JSON.stringify({
        operation: 'connect',
        device: {
          _id: deviceId
        }
      })

      queue.publish(data)
        .then(() => {
          resolve()
        }).catch((err) => {
          reject(err)
        })
    })
  }

  notifyDisconnection (deviceId) {
    let queueName = this.QN_AGENT_DEVICES
    let queue = this.queues[queueName]

    return new Promise((resolve, reject) => {
      if (!deviceId) {
        return reject(new Error('Kindly specify the device identifier'))
      }

      let data = JSON.stringify({
        operation: 'disconnect',
        device: {
          _id: deviceId
        }
      })

      queue.publish(data)
        .then(() => {
          resolve()
        }).catch((err) => {
          reject(err)
        })
    })
  }

  requestDeviceInfo (deviceId) {
    let queueName = this.QN_AGENT_DEVICE_INFO
    let queue = this.queues[queueName]

    return new Promise((resolve, reject) => {
      if (!deviceId) return reject(new Error('Kindly specify the device identifier'))

      queue.publish(JSON.stringify({device: { _id: deviceId }}))
        .then((reply) => {
          resolve(reply)
        }).catch((err) => {
          reject(err)
        })
    })
  }

  syncDevice (deviceInfo, deviceType) {
    let queueName = this.QN_AGENT_DEVICES
    let queue = this.queues[queueName]

    return new Promise((resolve, reject) => {
      if (!deviceInfo) {
        return reject(new Error('Kindly specify the device information/details'))
      }
      if (!(hasProp(deviceInfo, '_id') || hasProp(deviceInfo, 'id'))) {
        return reject(new Error('Kindly specify a valid id for the device'))
      }
      if (!hasProp(deviceInfo, 'name')) {
        return reject(new Error('Kindly specify a valid name for the device'))
      }

      let data = JSON.stringify({
        operation: 'sync',
        data: {
          type: deviceType, // Optional
          device: deviceInfo
        }
      })

      queue.publish(data)
        .then(() => {
          resolve()
        }).catch((err) => {
          reject(err)
        })
    })
  }

  removeDevice (deviceId) {
    let queueName = this.QN_AGENT_DEVICES
    let queue = this.queues[queueName]

    return new Promise((resolve, reject) => {
      if (!deviceId) {
        return reject(new Error('Kindly specify the device identifier'))
      }

      let data = JSON.stringify({
        operation: 'remove',
        device: {
          _id: deviceId
        }
      })

      queue.publish(data)
        .then(() => {
          resolve()
        }).catch((err) => {
          reject(err)
        })
    })
  }

  setDeviceState (deviceId, state) {
    let queueName = this.QN_AGENT_DEVICES
    let queue = this.queues[queueName]

    return new Promise((resolve, reject) => {
      if (!deviceId) {
        return reject(new Error('Kindly specify the device identifier'))
      }
      if (isNil(state) || isEmpty(state)) {
        return reject(new Error('Kindly specify the device state'))
      }

      let data = JSON.stringify({
        operation: 'setstate',
        device: {
          _id: deviceId,
          state: state
        }
      })

      queue.publish(data)
        .then(() => {
          resolve()
        }).catch((err) => {
          reject(err)
        })
    })
  }

  log (logData) {
    let self = this

    return new Promise((resolve, reject) => {
      if (isEmpty(logData)) return reject(new Error('Kindly specify the data to log'))

      // loggers and custom loggers are in self.loggers array
      async.each(self.qn.loggers, (loggerId, callback) => {
        if (!loggerId) return callback()

        // publish() has a built in stringify, so objects are safe to feed
        self.queues[loggerId].publish(logData)
          .then(() => {
            resolve()
          }).catch((err) => {
            reject(err)
          })
      }, (err) => {
        if (err) return reject(err)
        resolve()
      })
    })
  }

  logException (err) {
    let self = this

    return new Promise((resolve, reject) => {
      if (!(err instanceof Error)) return reject(new Error('Kindly specify a valid error to log'))

      let data = JSON.stringify({
        name: err.name,
        message: err.message,
        stack: err.stack
      })

      async.each(self.qn.exceptionLoggers, (loggerId, callback) => {
        if (!loggerId) return callback()

        self.queues[loggerId].publish(data)
          .then(() => {
            resolve()
          }).catch((err) => {
            reject(err)
          })
      }, (err) => {
        if (err) return reject(err)
        resolve()
      })
    })
  }
}

module.exports = Gateway
