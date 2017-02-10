'use strict'

const fs = require('fs')
const path = require('path')
const async = require('async')
const Promise = require('bluebird')

const isNil = require('lodash.isnil')
const hasProp = require('lodash.has')
const isError = require('lodash.iserror')
const isEmpty = require('lodash.isempty')
const isString = require('lodash.isstring')

const Broker = require('../broker.lib')
const EventEmitter = require('events').EventEmitter

class Gateway extends EventEmitter {

  constructor () {
    super()
    this.config = {}
    this.port = process.env.PORT || 8080

    const ENV_CA = process.env.CA || ''
    const ENV_CRL = process.env.CRL || ''
    const ENV_KEY = process.env.KEY || ''
    const ENV_CERT = process.env.CERT || ''
    const ENV_ACCOUNT = process.env.ACCOUNT || ''

    const QN_LOGS = 'logs'
    const QN_DATA = 'data'
    const QN_DEVICES = 'devices'
    const QN_MESSAGES = 'messages'
    const QN_DEVICE_INFO = 'deviceinfo'
    const QN_EXCEPTIONS = 'exceptions'
    const QN_PLUGIN_ID = process.env.PLUGIN_ID || 'demo.gateway'
    const QN_PIPELINE = process.env.PIPELINE || 'demo.pipeline'

    let _queues = []
    let _broker = new Broker()

    let _config = process.env.CONFIG || '{}'
    let _loggerIDs = process.env.LOGGERS || ''
    let _outputPipes = process.env.OUTPUT_PIPES || ''
    let _exLoggerIDs = process.env.EXCEPTION_LOGGERS || ''
    let _brokerConnStr = process.env.BROKER || 'amqp://guest:guest@127.0.0.1/'

    // consolidated queue names for one loop initialization
    let _qGroups = {
      loggers: [QN_LOGS],
      exceptionLoggers: [QN_EXCEPTIONS],
      common: [QN_DATA, QN_DEVICES, QN_MESSAGES, QN_PLUGIN_ID]
    }

    // env clean up, avoiding disclosure
    process.env.CA = undefined
    process.env.CRL = undefined
    process.env.KEY = undefined
    process.env.CERT = undefined
    process.env.PORT = undefined
    process.env.BROKER = undefined
    process.env.CONFIG = undefined
    process.env.ACCOUNT = undefined
    process.env.LOGGERS = undefined
    process.env.PIPELINE = undefined
    process.env.PLUGIN_ID = undefined
    process.env.OUTPUT_PIPES = undefined
    process.env.EXCEPTION_LOGGERS = undefined

    // adding custom queues in consolidated queue names '_qGroups.*'
    _qGroups.exceptionLoggers = _qGroups.exceptionLoggers.concat(_exLoggerIDs.split(','))
    _qGroups.outPipes = _qGroups.loggers.concat(_outputPipes.split(','))
    _qGroups.loggers = _qGroups.loggers.concat(_loggerIDs.split(','))

    // removing empty elements, if any
    _qGroups.exceptionLoggers = _qGroups.exceptionLoggers.filter(Boolean)
    _qGroups.outPipes = _qGroups.outPipes.filter(Boolean)
    _qGroups.loggers = _qGroups.loggers.filter(Boolean)

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
          this.config = parsed
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
          ca: (callback) => { writer(ENV_CA, path.join(home, 'server-ca.pem'), callback) },
          crl: (callback) => { writer(ENV_CRL, path.join(home, 'server-crl.pem'), callback) },
          key: (callback) => { writer(ENV_KEY, path.join(home, 'server-key.pem'), callback) },
          cert: (callback) => { writer(ENV_CERT, path.join(home, 'server-cert.pem'), callback) }
        }, (err, res) => {
          Object.assign(this, res)
          done(err)
        })
      },

      // setting up needed queues
      (done) => {
        let queueIDs = []

        queueIDs = queueIDs.concat(_qGroups.common)
        queueIDs = queueIDs.concat(_qGroups.loggers)
        queueIDs = queueIDs.concat(_qGroups.outPipes)
        queueIDs = queueIDs.concat(_qGroups.exceptionLoggers)

        async.each(queueIDs, (loggerId, callback) => {
          if (isEmpty(loggerId)) return callback()

          _broker.newQueue(loggerId)
            .then((queue) => {
              if (queue) _queues[loggerId] = queue
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
        let processTopicData = (msg) => {
          if (!isEmpty(msg)) {
            async.waterfall([
              async.constant(msg.content.toString('utf8')),
              async.asyncify(JSON.parse)
            ], (err, parsed) => {
              if (!err) return this.emit('message', parsed)
              console.error('Gateway processQueue() rcvd data is not a JSON.', err)
            })
          }
        }

        _broker.newExchangeQueue(QN_PIPELINE)
          .then((queue) => {
            if (!queue) throw new Error('newExchangeQueue() fail')
            _queues[QN_PIPELINE] = queue
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
        _broker.newRpc('client', QN_DEVICE_INFO)
          .then((queue) => {
            let err = queue ? null : new Error("Can't setup rpc queue.")
            if (queue) _queues[QN_DEVICE_INFO] = queue
            return done(err) || null
          }).catch((err) => {
            done(err)
          })
      }

      // plugin initialized
    ], (err) => {
      if (err) return console.error('Gateway: ', err)
      this.emit('ready')
    })

    this.pipe = (data, sequenceId) => {
      return new Promise((resolve, reject) => {
        if (!data) return reject(new Error('Kindly specify the data to forward'))

        let packet = {
          plugin: QN_PLUGIN_ID,
          pipe: _outputPipes,
          sequenceId: sequenceId,
          data: data
        }

        let sendToPipes = () => {
          async.each(_qGroups.outPipes, (pipe, callback) => {
            if (!pipe) return callback()
            _queues[pipe].publish(packet)
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
          _queues[QN_DATA].publish(packet)
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

    this.relayMessage = (message, deviceTypes, devices) => {
      return new Promise((resolve, reject) => {
        if (!message) {
          return reject(new Error('Kindly specify the command/message to send'))
        }
        if (isEmpty(devices) && isEmpty(deviceTypes)) {
          return reject(new Error('Kindly specify the target device types or devices'))
        }

        if ((isString(devices) || Array.isArray(devices)) &&
          (isString(deviceTypes) || Array.isArray(deviceTypes))) {

          _queues[QN_MESSAGES].publish({
            pipeline: QN_PIPELINE,
            message: message,
            deviceTypes: deviceTypes,
            devices: devices
          }).then(() => {
            resolve()
          }).catch((err) => {
            reject(err)
          })

        } else {
          return reject(new Error("'devices' and 'deviceTypes' must be a string or an array."))
        }
      })
    }

    this.notifyConnection = (deviceId) => {
      return new Promise((resolve, reject) => {
        if (!deviceId) {
          return reject(new Error('Kindly specify the device identifier'))
        }
        if (!isString(deviceId)) {
          return reject(new Error('Device identifier must be a string'))
        }

        _queues[QN_DEVICES].publish({
          operation: 'connect',
          account: ENV_ACCOUNT,
          device: { _id: deviceId }
        }).then(() => {
          resolve()
        }).catch((err) => {
          reject(err)
        })

      })
    }

    this.notifyDisconnection = (deviceId) => {
      return new Promise((resolve, reject) => {
        if (!deviceId) {
          return reject(new Error('Kindly specify the device identifier'))
        }
        if (!isString(deviceId)) {
          return reject(new Error('Device identifier must be a string'))
        }

        _queues[QN_DEVICES].publish({
          operation: 'disconnect',
          account: ENV_ACCOUNT,
          device: { _id: deviceId }
        }).then(() => {
          resolve()
        }).catch((err) => {
          reject(err)
        })

      })
    }

    this.requestDeviceInfo = (deviceId) => {

      return new Promise((resolve, reject) => {
        if (!deviceId) {
          return reject(new Error('Kindly specify the device identifier'))
        }
        if (!isString(deviceId)) {
          return reject(new Error('Device identifier must be a string'))
        }

        _queues[QN_DEVICE_INFO].publish({
          account: ENV_ACCOUNT,
          device: { _id: deviceId }
        }).then((reply) => {
          resolve(reply)
        }).catch((err) => {
          reject(err)
        })

      })
    }

    this.syncDevice = (deviceInfo, deviceType) => {
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

        _queues[QN_DEVICES].publish({
          operation: 'sync',
          account: ENV_ACCOUNT,
          data: { type: deviceType, device: deviceInfo }
        }).then(() => {
          resolve()
        }).catch((err) => {
          reject(err)
        })

      })
    }

    this.removeDevice = (deviceId) => {
      return new Promise((resolve, reject) => {
        if (!deviceId) {
          return reject(new Error('Kindly specify the device identifier'))
        }
        if (!isString(deviceId)) {
          return reject(new Error('Device identifier must be a string'))
        }

        _queues[QN_DEVICES].publish({
          operation: 'remove',
          account: ENV_ACCOUNT,
          device: { _id: deviceId }
        }).then(() => {
          resolve()
        }).catch((err) => {
          reject(err)
        })

      })
    }

    this.setDeviceState = (deviceId, state) => {
      return new Promise((resolve, reject) => {
        if (!deviceId) {
          return reject(new Error('Kindly specify the device identifier'))
        }
        if (!isString(deviceId)) {
          return reject(new Error('Device identifier must be a string'))
        }
        if (isNil(state) || isEmpty(state)) {
          return reject(new Error('Kindly specify the device state'))
        }

        _queues[QN_DEVICES].publish({
          operation: 'setstate',
          account: ENV_ACCOUNT,
          device: { _id: deviceId, state: state }
        }).then(() => {
          resolve()
        }).catch((err) => {
          reject(err)
        })

      })
    }

    this.log = (logData) => {
      let msg = null

      return new Promise((resolve, reject) => {
        if (isEmpty(logData)) return reject(new Error('Kindly specify the data to log'))

        logData = {
          account: ENV_ACCOUNT,
          rkhLogData: logData
        }

        // loggers and custom loggers are in _qGroups.loggers array
        async.each(_qGroups.loggers, (loggerId, callback) => {
          if (!loggerId) return callback()

          msg = loggerId === QN_LOGS
            ? { account: ENV_ACCOUNT, data: logData }
            : logData

          // publish() has a built in stringify, so objects are safe to feed
          _queues[loggerId].publish(msg)
            .then(() => {
              callback()
            }).catch((err) => {
              reject(err)
            })

        }, (err) => {
          if (err) return reject(err)
          resolve()
        })
      })
    }

    this.logException = (err) => {
      let msg = null

      let errData = {
        name: err.name,
        message: err.message,
        stack: err.stack
      }

      return new Promise((resolve, reject) => {
        if (!isError(err)) return reject(new Error('Kindly specify a valid error to log'))

        async.each(_qGroups.exceptionLoggers, (loggerId, callback) => {
          if (!loggerId) return callback()

          msg = loggerId === QN_EXCEPTIONS
            ? { account: ENV_ACCOUNT, data: errData }
            : errData

          _queues[loggerId].publish(msg)
            .then(() => {
              callback()
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
}

module.exports = Gateway
