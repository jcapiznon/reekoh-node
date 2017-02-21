'use strict'

const fs = require('fs')
const path = require('path')
const async = require('async')
const BPromise = require('bluebird')
const EventEmitter = require('events').EventEmitter

const isEmpty = require('lodash.isempty')
const isError = require('lodash.iserror')
const isString = require('lodash.isstring')
const isPlainObject = require('lodash.isplainobject')

const Broker = require('../broker.lib')

class Channel extends EventEmitter {

  constructor () {
    super()
    this.config = {}
    this.port = process.env.PORT || 8080

    const BROKER = process.env.BROKER
    const ACCOUNT = process.env.ACCOUNT
    const PLUGIN_ID = process.env.PLUGIN_ID
    const INPUT_PIPE = process.env.INPUT_PIPE

    const LOGGERS = `${process.env.LOGGERS || ''}`.split(',').filter(Boolean)
    const COMMAND_RELAYS = `${process.env.COMMAND_RELAYS || ''}`.split(',').filter(Boolean)
    const EXCEPTION_LOGGERS = `${process.env.EXCEPTION_LOGGERS || ''}`.split(',').filter(Boolean)

    let _broker = new Broker()

    process.env.PORT = undefined
    process.env.BROKER = undefined
    process.env.ACCOUNT = undefined
    process.env.LOGGERS = undefined
    process.env.PIPELINE = undefined
    process.env.INPUT_PIPE = undefined
    process.env.COMMAND_RELAYS = undefined
    process.env.EXCEPTION_LOGGERS = undefined

    async.series([
      // connecting to rabbitMQ
      (done) => {
        _broker.connect(BROKER).then(() => {
          return done() || null // promise warning fix
        }).catch(done)
      },

      // parse config trap error
      (done) => {
        async.waterfall([
          async.constant(process.env.CONFIG || '{}'),
          async.asyncify(JSON.parse)
        ], (err, parsed) => {
          if (!err) {
            this.config = parsed
            process.env.CONFIG = undefined
          }
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

        async.parallel({
          ca: (cb) => { writer(process.env.CA, path.join(home, 'server-ca.pem'), cb) },
          crl: (cb) => { writer(process.env.CRL, path.join(home, 'server-crl.pem'), cb) },
          key: (cb) => { writer(process.env.KEY, path.join(home, 'server-key.pem'), cb) },
          cert: (cb) => { writer(process.env.CERT, path.join(home, 'server-cert.pem'), cb) }
        }, (err, res) => {
          Object.assign(this, res)

          process.env.CA = undefined
          process.env.CRL = undefined
          process.env.KEY = undefined
          process.env.CERT = undefined

          done(err)
        })
      },

      // setting up needed queues
      (done) => {
        let queueIds = [INPUT_PIPE, 'logs', 'exceptions']
          .concat(EXCEPTION_LOGGERS)
          .concat(COMMAND_RELAYS)
          .concat(LOGGERS)

        async.each(queueIds, (loggerId, cb) => {
          _broker.createQueue(loggerId).then(() => {
            return cb() || null // promise warning fix
          }).catch((err) => {
            console.error('Channel newQueue() ', err)
            cb(err)
          })
        }, done)
      },

      // set topic exchnage queue and consume
      (done) => {
        let processTopicData = (msg) => {
          if (!isEmpty(msg)) {
            async.waterfall([
              async.constant(msg.content.toString('utf8')),
              async.asyncify(JSON.parse)
            ], (err, parsed) => {
              if (err) return console.error('Channel processQueue() rcvd data is not a JSON.', err)

              this.emit('data', parsed)
            })
          }
        }

        _broker.createExchange(PLUGIN_ID).then((exchange) => {
          return exchange.consume(processTopicData)
        }).then(() => {
          return done() || null // promise warning fix
        }).catch(done)
      },

      // listen to input pipe, then relay to plugin queue
      (done) => {
        _broker.queues[INPUT_PIPE].consume((msg) => {
          _broker.exchanges[PLUGIN_ID].publish(msg.content.toString('utf8'))
        }).then(() => {
          done()
        }).catch(done)
      }

    ], (err) => {

      if (err) {
        console.error('Channel: ', err)
        throw err
      }

      process.nextTick(() => {
        // plugin initialized
        this.emit('ready')
      })
    })

    this.relayCommand = (command, devices, deviceGroups) => {
      return new BPromise((resolve, reject) => {
        if (!command || !isString(command)) {
          return reject(new Error('Kindly specify a valid command to send'))
        }

        if (isEmpty(devices) && isEmpty(deviceGroups)) {
          return reject(new Error('Kindly specify the target device types or devices'))
        }

        if (!(isString(deviceGroups) || Array.isArray(deviceGroups)) || !(isString(devices) || Array.isArray(devices))) {
          return reject(new Error(`'deviceGroups' and 'devices' must be a string or an array.`))
        }

        async.each(COMMAND_RELAYS, (cmdId, cb) => {
          _broker.queues[cmdId].publish({
            command: command,
            deviceGroups: deviceGroups,
            devices: devices
          }).then(() => {
            cb()
          }).catch(reject)
        }, resolve)

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
                done()
              }).catch(done)
            }, callback)
          },
          (callback) => {
            _broker.queues['logs'].publish({
              account: ACCOUNT,
              data: logData
            }).then(() => {
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
                done()
              }).catch(done)
            }, callback)
          },
          (callback) => {
            _broker.queues['exceptions'].publish({
              account: ACCOUNT,
              data: errData
            }).then(() => {
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

module.exports = Channel
