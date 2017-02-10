'use strict'

const fs = require('fs')
const path = require('path')
const async = require('async')
const Promise = require('bluebird')

const isArray = Array.isArray
const isEmpty = require('lodash.isempty')
const isError = require('lodash.iserror')
const isString = require('lodash.isstring')

const Broker = require('../broker.lib')
const EventEmitter = require('events').EventEmitter

class Channel extends EventEmitter {

  constructor () {
    super()
    this.config = {}
    this.port = process.env.PORT || 8080

    const ENV_CA = process.env.CA || ''
    const ENV_CRL = process.env.CRL || ''
    const ENV_KEY = process.env.KEY || ''
    const ENV_CERT = process.env.CERT || ''
    const ENV_ACCOUNT = process.env.ACCOUNT || ''
    const ENV_PIPELINE = process.env.PIPELINE || ''

    const QN_LOGS = 'logs'
    const QN_MESSAGES = 'messages'
    const QN_EXCEPTIONS = 'exceptions'
    const QN_PLUGIN_ID = process.env.PLUGIN_ID || 'demo.channel'
    const QN_INPUT_PIPE = process.env.INPUT_PIPE || 'demo.channel.pipe'

    let _queues = []
    let _broker = new Broker()

    let _config = process.env.CONFIG || '{}'
    let _loggerIDs = process.env.LOGGERS || ''
    let _brokerConnStr = process.env.BROKER || 'amqp://guest:guest@127.0.0.1/'
    let _exLoggerIDs = process.env.EXCEPTION_LOGGERS || ''

    // consolidated queue names for one loop initialization
    let _qGroups = {
      loggers: [QN_LOGS],
      exceptionLoggers: [QN_EXCEPTIONS],
      common: [QN_INPUT_PIPE, QN_MESSAGES]
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
    process.env.INPUT_PIPE = undefined
    process.env.EXCEPTION_LOGGERS = undefined

    // adding custom queues in consolidated queue names '_qGroups.*'
    _qGroups.loggers = _qGroups.loggers.concat(_loggerIDs.split(','))
    _qGroups.exceptionLoggers = _qGroups.exceptionLoggers.concat(_exLoggerIDs.split(','))
    
    // removing empty elements if any
    _qGroups.loggers = _qGroups.loggers.filter(Boolean)
    _qGroups.exceptionLoggers = _qGroups.exceptionLoggers.filter(Boolean)

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
        queueIDs = queueIDs.concat(_qGroups.exceptionLoggers)

        async.each(queueIDs, (loggerId, callback) => {
          if (isEmpty(loggerId)) return callback()

          _broker.newQueue(loggerId)
            .then((queue) => {
              if (queue) _queues[loggerId] = queue
              return callback() || null // !
            }).catch((err) => {
              console.error('Channel newQueue()', err)
            })
        }, (err) => {
          done(err)
        })
      },

      // set topic exchnage queue and consume
      (done) => {

        let processTopicData = (msg) => {
          if (!isEmpty(msg)) {
            async.waterfall([
              async.constant(msg.content.toString('utf8')),
              async.asyncify(JSON.parse)
            ], (err, parsed) => {
              if (!err) return this.emit('data', parsed)
              console.error('Channel processQueue() rcvd data is not a JSON.', err)
            })
          }
        }

        _broker.newExchangeQueue(QN_PLUGIN_ID)
          .then((queue) => {
            if (!queue) throw new Error('newExchangeQueue() fail') // will be catch by async
            _queues[QN_PLUGIN_ID] = queue
            return queue
          }).then((queue) => {
            return queue.consume(processTopicData)
          }).then(() => {
            return done() || null // !
          }).catch((err) => {
            done(err)
          })
      },

      // listen to input pipe, then relay to plugin queue
      (done) => {
        _queues[QN_INPUT_PIPE].consume((msg) => {
          _queues[QN_PLUGIN_ID].publish(msg.content.toString('utf8'))
        }).then(() => {
          return done() || null
        }).catch((err) => {
          done(err)
        })
      }

    ], (err) => {
      if (err) return console.error('Channel: ', err)

      // plugin initialized
      this.emit('ready')
    })

    this.relayMessage = (message, deviceTypes, devices) => {

      return new Promise((resolve, reject) => {
        if (!message) {
          return reject(new Error('Kindly specify the command/message to send'))
        }
        if (!isString(message)) {
          return reject(new Error('Message must be a string'))
        }
        if (isEmpty(devices) && isEmpty(deviceTypes)) {
          return reject(new Error('Kindly specify the target device types or devices'))
        }
        if (!(isString(deviceTypes) || isArray(deviceTypes)) || !(isString(devices) || isArray(devices))) {
          return reject(new Error('Device types and devices must be a string or array'))
        }

        if ((isString(devices) || Array.isArray(devices)) &&
          (isString(deviceTypes) || Array.isArray(deviceTypes))) {

          _queues[QN_MESSAGES].publish({
            pipeline: ENV_PIPELINE,
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

    this.log = (logData) => {
      let msg = null

      return new Promise((resolve, reject) => {
        if (isEmpty(logData)) {
          return reject(new Error('Kindly specify the data to log'))
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

module.exports = Channel
