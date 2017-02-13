'use strict'

const fs = require('fs')
const path = require('path')
const async = require('async')
const BPromise = require('bluebird')
const EventEmitter = require('events').EventEmitter

const isNil = require('lodash.isnil')
const hasProp = require('lodash.has')
const isError = require('lodash.iserror')
const isEmpty = require('lodash.isempty')
const isString = require('lodash.isstring')
const isPlainObject = require('lodash.isplainobject')

const Broker = require('../broker.lib')

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

    async.series([
      // connecting to rabbitMQ
      (done) => {
        _broker.connect(_brokerConnStr).then(() => {
          return done()
        }).catch(done)
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

        let writer = (content, filePath, cb) => {
          if (isEmpty(content)) return cb(null, '')
          fs.writeFile(filePath, content, (err) => { cb(err, filePath) })
        }

        async.series({
          ca: (cb) => { writer(ENV_CA, path.join(home, 'server-ca.pem'), cb) },
          crl: (cb) => { writer(ENV_CRL, path.join(home, 'server-crl.pem'), cb) },
          key: (cb) => { writer(ENV_KEY, path.join(home, 'server-key.pem'), cb) },
          cert: (cb) => { writer(ENV_CERT, path.join(home, 'server-cert.pem'), cb) }
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

        async.each(queueIDs, (loggerId, cb) => {
          if (isEmpty(loggerId)) return cb()

          _broker.createQueue(loggerId).then(() => {
            return cb()
          }).catch((err) => {
            console.error('Gateway newQueue()', err)
            cb(err)
          })
        }, done)
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

        _broker.createExchange(QN_PIPELINE).then((exchange) => {
          return exchange.consume(processTopicData)
        }).then(() => {
          return done()
        }).catch(done)
      },

      // setting up RPC queue
      (done) => {
        _broker.createRPC('client', QN_DEVICE_INFO).then((queue) => {
          return done((queue) ? null : new Error('Can\'t setup rpc queue.'))
        }).catch(done)
      }

      // plugin initialized
    ], (err) => {
      if (err) {
        console.error('Gateway: ', err)
        throw err
      }

      process.nextTick(() => {
        // plugin initialized
        this.emit('ready')
      })
    })

    this.pipe = (data, sequenceId) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(data) || !isPlainObject(data)) return reject(new Error('Invalid data received. Data should be and Object and should not empty.'))

        if (sequenceId && !isString(sequenceId)) return reject(new Error('Kindly specify a valid sequence id'))

        if (sequenceId) {
          _broker.queues[QN_DATA].publish({
            plugin: QN_PLUGIN_ID,
            pipe: _outputPipes,
            sequenceId: sequenceId,
            data: data
          }).then(() => {
            resolve()
          }).catch(reject)
        } else {
          async.each(_qGroups.outPipes, (pipe, callback) => {
            if (!pipe) return callback()
            _broker.queues[pipe].publish(data).then(() => {
              callback()
            }).catch(callback)
          }, (err) => {
            if (err) return reject(err)
            resolve()
          })
        }
      })
    }

    this.relayMessage = (message, deviceTypes, devices) => {
      return new BPromise((resolve, reject) => {
        if (!message) {
          return reject(new Error('Kindly specify the command/message to send'))
        }

        if (isEmpty(devices) && isEmpty(deviceTypes)) {
          return reject(new Error('Kindly specify the target device types or devices'))
        }

        if (!(isString(deviceTypes) || Array.isArray(deviceTypes)) || !(isString(devices) || Array.isArray(devices))) {
          return reject(new Error(`'deviceTypes' and 'devices' must be a string or an array.`))
        }

        _broker.queues[QN_MESSAGES].publish({
          pipeline: QN_PIPELINE,
          message: message,
          deviceTypes: deviceTypes,
          devices: devices
        }).then(() => {
          resolve()
        }).catch(reject)
      })
    }

    this.notifyConnection = (deviceId) => {
      return new BPromise((resolve, reject) => {
        if (!deviceId) {
          return reject(new Error('Kindly specify the device identifier'))
        }
        if (!isString(deviceId)) {
          return reject(new Error('Device identifier must be a string'))
        }

        _broker.queues[QN_DEVICES].publish({
          operation: 'connect',
          account: ENV_ACCOUNT,
          device: {
            _id: deviceId
          }
        }).then(() => {
          resolve()
        }).catch(reject)
      })
    }

    this.notifyDisconnection = (deviceId) => {
      return new BPromise((resolve, reject) => {
        if (!deviceId) {
          return reject(new Error('Kindly specify the device identifier'))
        }
        if (!isString(deviceId)) {
          return reject(new Error('Device identifier must be a string'))
        }

        _broker.queues[QN_DEVICES].publish({
          operation: 'disconnect',
          account: ENV_ACCOUNT,
          device: {
            _id: deviceId
          }
        }).then(() => {
          resolve()
        }).catch(reject)
      })
    }

    this.requestDeviceInfo = (deviceId) => {
      return new BPromise((resolve, reject) => {
        if (!deviceId) {
          return reject(new Error('Kindly specify the device identifier'))
        }

        if (!isString(deviceId)) {
          return reject(new Error('Device identifier must be a string'))
        }

        _broker.rpcs[QN_DEVICE_INFO].publish({
          account: ENV_ACCOUNT,
          device: {
            _id: deviceId
          }
        }).then((reply) => {
          resolve(reply)
        }).catch(reject)
      })
    }

    this.syncDevice = (deviceInfo, deviceType) => {
      return new BPromise((resolve, reject) => {
        if (!deviceInfo) {
          return reject(new Error('Kindly specify the device information/details'))
        }

        if (!(hasProp(deviceInfo, '_id') || hasProp(deviceInfo, 'id'))) {
          return reject(new Error('Kindly specify a valid id for the device'))
        }

        if (!hasProp(deviceInfo, 'name')) {
          return reject(new Error('Kindly specify a valid name for the device'))
        }

        _broker.queues[QN_DEVICES].publish({
          operation: 'sync',
          account: ENV_ACCOUNT,
          data: {
            type: deviceType,
            device: deviceInfo
          }
        }).then(() => {
          resolve()
        }).catch(reject)
      })
    }

    this.removeDevice = (deviceId) => {
      return new BPromise((resolve, reject) => {
        if (!deviceId) {
          return reject(new Error('Kindly specify the device identifier'))
        }
        if (!isString(deviceId)) {
          return reject(new Error('Device identifier must be a string'))
        }

        _broker.queues[QN_DEVICES].publish({
          operation: 'remove',
          account: ENV_ACCOUNT,
          device: {
            _id: deviceId
          }
        }).then(() => {
          resolve()
        }).catch(reject)
      })
    }

    this.setDeviceState = (deviceId, state) => {
      return new BPromise((resolve, reject) => {
        if (!deviceId) {
          return reject(new Error('Kindly specify the device identifier'))
        }

        if (!isString(deviceId)) {
          return reject(new Error('Device identifier must be a string'))
        }

        if (isNil(state) || isEmpty(state)) {
          return reject(new Error('Kindly specify the device state'))
        }

        _broker.queues[QN_DEVICES].publish({
          operation: 'setstate',
          account: ENV_ACCOUNT,
          device: {
            _id: deviceId,
            state: state
          }
        }).then(() => {
          resolve()
        }).catch(reject)
      })
    }

    this.log = (logData) => {
      return new BPromise((resolve, reject) => {
        if (isEmpty(logData)) return reject(new Error('Kindly specify the data to log'))

        let msg = null

        // loggers and custom loggers are in _qGroups.loggers array
        async.each(_qGroups.loggers, (loggerId, callback) => {
          if (!loggerId) return callback()

          msg = (loggerId === QN_LOGS) ? {account: ENV_ACCOUNT, data: logData} : logData

          // publish() has a built in stringify, so objects are safe to feed
          _broker.queues[loggerId].publish(msg).then(() => {
            callback()
          }).catch(callback)

        }, (err) => {
          if (err) return reject(err)

          resolve()
        })
      })
    }

    this.logException = (err) => {
      return new BPromise((resolve, reject) => {
        if (!isError(err)) return reject(new Error('Kindly specify a valid error to log'))

        let msg = null

        let errData = {
          name: err.name,
          message: err.message,
          stack: err.stack
        }

        async.each(_qGroups.exceptionLoggers, (loggerId, callback) => {
          if (!loggerId) return callback()

          msg = (loggerId === QN_EXCEPTIONS) ? {account: ENV_ACCOUNT, data: errData} : errData

          _broker.queues[loggerId].publish(msg).then(() => {
            callback()
          }).catch(callback)

        }, (err) => {
          if (err) return reject(err)

          resolve()
        })
      })
    }
  }
}

module.exports = Gateway
