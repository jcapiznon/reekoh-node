/* global describe, it, before, after */

'use strict'

let _conn = null
let _broker = null
let _plugin = null
let _channel = null

const async = require('async')
const amqp = require('amqplib')
const reekoh = require('../../index.js')
const isEqual = require('lodash.isequal')
const Broker = require('../../lib/broker.lib')

const QN_AGENT_DEVICE_INFO = 'agent.deviceinfo'
const ERR_RETURN_UNMATCH = 'Returned value not matched.'
const ERR_EXPECT_REJECTION = 'Expecting rejection. check function test param.'
const ERR_EMPTY_IDENTIFIER = 'Kindly specify the device identifier'
const ERR_EMPTY_DEVICE_ID = 'Kindly specify a valid id for the device'
const ERR_EMPTY_DEVICE_NAME = 'Kindly specify a valid name for the device'
const ERR_EMPTY_DEVICE_INFO = 'Kindly specify the device information/details'
const ERR_EMPTY_DEVICE_STATE = 'Kindly specify the device state'
const ERR_EMPTY_DATA_TO_FORWARD = 'Kindly specify the data to forward'
const ERR_EMPTY_CMD_TO_SEND = 'Kindly specify the command/message to send'
const ERR_EMPTY_DEVICE_OR_DEVICE_TYPES = 'Kindly specify the target device types or devices'

describe('Gateway Plugin Test', () => {
  before('#test init', () => {
    process.env.LOGGERS = ''
    process.env.EXCEPTION_LOGGERS = ''

    process.env.PIPELINE = 'demo.pipeline'
    process.env.OUTPUT_PIPES = 'outpipe.1,outpipe.2'
    process.env.BROKER = 'amqp://guest:guest@127.0.0.1/'

    _broker = new Broker() // tester broker

    amqp.connect(process.env.BROKER)
      .then((conn) => {
        _conn = conn
        return conn.createChannel()
      }).then((channel) => {
        _channel = channel
      }).catch((err) => {
        console.log(err)
      })
  })

  after('terminate connection', () => {
    _conn.close()
  })

  describe('#spawn', () => {
    it('should spawn the class without error', (done) => {
      try {
        _plugin = new reekoh.plugins.Gateway()
        done()
      } catch (err) {
        done(err)
      }
    })
  })

  describe('#events', () => {
    it('should rcv `ready` event', (done) => {
      _plugin.once('ready', () => {
        done()
      })
    })

    it('should rcv `message` event', (done) => {
      let dummyData = { 'foo': 'bar' }
      _channel.sendToQueue(process.env.PIPELINE, new Buffer(JSON.stringify(dummyData)))

      _plugin.on('message', (data) => {
        if (!isEqual(data, dummyData)) {
          done(new Error(ERR_RETURN_UNMATCH))
        } else {
          done()
        }
      })
    })
  })

  describe('#RPC', () => {
    it('should connect to broker', (done) => {
      _broker.connect(process.env.BROKER)
        .then(() => {
          return done()
        }).catch((err) => {
          done(err)
        })
    })

    it('should spawn temporary RPC server', (done) => {
      // if request arrives this proc will be called
      let sampleServerProcedure = (msg) => {
        return new Promise((resolve, reject) => {
          async.waterfall([
            async.constant(msg.content.toString('utf8')),
            async.asyncify(JSON.parse)
          ], (err, parsed) => {
            if (err) return reject(err)
            parsed.foo = 'bar'
            resolve(JSON.stringify(parsed))
          })
        })
      }

      _broker.newRpc('server', QN_AGENT_DEVICE_INFO)
        .then((queue) => {
          return queue.serverConsume(sampleServerProcedure)
        }).then(() => {
          // Awaiting RPC requests
          done()
        }).catch((err) => {
          done(err)
        })
    })

    describe('.requestDeviceInfo()', () => {
      it('should throw error if deviceId is empty', (done) => {
        _plugin.requestDeviceInfo('', () => {})
          .then(() => {
            // noop!
          }).catch((err) => {
            if (!isEqual(err.message, ERR_EMPTY_IDENTIFIER)) {
              done(new Error(ERR_RETURN_UNMATCH))
            } else {
              done()
            }
          })
      })

      it('should request device info', (done) => {
        _plugin.requestDeviceInfo(123)
          .then((ret) => {
            async.waterfall([
              async.constant(ret),
              async.asyncify(JSON.parse)
            ], (err, parsed) => {
              done(err)
            })
          }).catch((err) => {
            done(err)
          })
      })
    })
  })

  describe('#pipe()', () => {
    it('should throw error if data is empty', (done) => {
      _plugin.pipe('', '')
        .then(() => {
          done(new Error(ERR_EXPECT_REJECTION))
        }).catch((err) => {
          if (!isEqual(err.message, ERR_EMPTY_DATA_TO_FORWARD)) {
            done(new Error(ERR_RETURN_UNMATCH))
          } else {
            done()
          }
        })
    })

    it('should publish data to output pipes', (done) => {
      _plugin.pipe('{"foo":"bar"}', '') // no seq
        .then(() => {
          done()
        }).catch((err) => {
          done(err)
        })
    })

    it('should publish data to sanitizer', (done) => {
      _plugin.pipe('{"foo":"bar"}', 'seq123')
        .then(() => {
          done()
        }).catch((err) => {
          done(err)
        })
    })
  })

  describe('#relayMessage()', () => {
    it('should throw error if message is empty', (done) => {
      _plugin.relayMessage('', '', '')
        .then(() => {
          done(new Error(ERR_EXPECT_REJECTION))
        }).catch((err) => {
          if (!isEqual(err.message, ERR_EMPTY_CMD_TO_SEND)) {
            done(new Error(ERR_RETURN_UNMATCH))
          } else {
            done()
          }
        })
    })

    it('should throw error if device or deviceTypes is empty', (done) => {
      _plugin.relayMessage('test', '', '')
        .then(() => {
          done(new Error(ERR_EXPECT_REJECTION))
        }).catch((err) => {
          if (!isEqual(err.message, ERR_EMPTY_DEVICE_OR_DEVICE_TYPES)) {
            done(new Error(ERR_RETURN_UNMATCH))
          } else {
            done()
          }
        })
    })

    it('should publish a message to `Message Relay Queue`', (done) => {
      _plugin.relayMessage('test', ['a'], ['b'])
        .then(() => {
          done()
        }).catch((err) => {
          done(err)
        })
    })
  })

  describe('#notifyConnection()', () => {
    it('should throw error if deviceId is empty', (done) => {
      _plugin.notifyConnection('')
        .then(() => {
          done(new Error(ERR_EXPECT_REJECTION))
        }).catch((err) => {
          if (!isEqual(err.message, ERR_EMPTY_IDENTIFIER)) {
            done(new Error(ERR_RETURN_UNMATCH))
          } else {
            done()
          }
        })
    })

    it('should publish a message to device', (done) => {
      _plugin.notifyConnection('test')
        .then(() => {
          done()
        }).catch((err) => {
          done(err)
        })
    })
  })

  describe('#notifyDisconnection()', () => {
    it('should throw error if deviceId is empty', (done) => {
      _plugin.notifyDisconnection('')
        .then(() => {
          done(new Error(ERR_EXPECT_REJECTION))
        }).catch((err) => {
          if (!isEqual(err.message, ERR_EMPTY_IDENTIFIER)) {
            done(new Error(ERR_RETURN_UNMATCH))
          } else {
            done()
          }
        })
    })

    it('should publish a message to device', (done) => {
      _plugin.notifyDisconnection('test')
        .then(() => {
          done()
        }).catch((err) => {
          done(err)
        })
    })
  })

  describe('#syncDevice()', () => {
    it('should throw error if deviceInfo is empty', (done) => {
      _plugin.syncDevice('', [])
        .then(() => {
          done(new Error(ERR_EXPECT_REJECTION))
        }).catch((err) => {
          if (!isEqual(err.message, ERR_EMPTY_DEVICE_INFO)) {
            done(new Error(ERR_RETURN_UNMATCH))
          } else {
            done()
          }
        })
    })

    it('should throw error if deviceInfo doesnt have `_id` or `id` property', (done) => {
      _plugin.syncDevice({}, [])
        .then(() => {
          done(new Error(ERR_EXPECT_REJECTION))
        }).catch((err) => {
          if (!isEqual(err.message, ERR_EMPTY_DEVICE_ID)) {
            done(new Error(ERR_RETURN_UNMATCH))
          } else {
            done()
          }
        })
    })

    it('should throw error if deviceInfo doesnt have `name` property', (done) => {
      _plugin.syncDevice({_id: 123}, [])
        .then(() => {
          done(new Error(ERR_EXPECT_REJECTION))
        }).catch((err) => {
          if (!isEqual(err.message, ERR_EMPTY_DEVICE_NAME)) {
            done(new Error(ERR_RETURN_UNMATCH))
          } else {
            done()
          }
        })
    })

    it('should publish sync msg to queue', (done) => {
      _plugin.syncDevice({_id: 123, name: 'foo'}, [])
        .then(() => {
          done()
        }).catch((err) => {
          done(err)
        })
    })
  })

  describe('#removeDevice()', () => {
    it('should throw error if deviceId is empty', (done) => {
      _plugin.removeDevice('')
        .then(() => {
          done(new Error(ERR_EXPECT_REJECTION))
        }).catch((err) => {
          if (!isEqual(err.message, ERR_EMPTY_IDENTIFIER)) {
            done(new Error(ERR_RETURN_UNMATCH))
          } else {
            done()
          }
        })
    })

    it('should publish remove msg to queue', (done) => {
      _plugin.removeDevice('test')
        .then(() => {
          done()
        }).catch((err) => {
          done(err)
        })
    })
  })

  describe('#setDeviceState()', () => {
    it('should throw error if deviceId is empty', (done) => {
      _plugin.setDeviceState('', '')
        .then(() => {
          done(new Error(ERR_EXPECT_REJECTION))
        }).catch((err) => {
          if (!isEqual(err.message, ERR_EMPTY_IDENTIFIER)) {
            done(new Error(ERR_RETURN_UNMATCH))
          } else {
            done()
          }
        })
    })

    it('should throw error if state is empty', (done) => {
      _plugin.setDeviceState('test', '')
        .then(() => {
          done(new Error(ERR_EXPECT_REJECTION))
        }).catch((err) => {
          if (!isEqual(err.message, ERR_EMPTY_DEVICE_STATE)) {
            done(new Error(ERR_RETURN_UNMATCH))
          } else {
            done()
          }
        })
    })

    it('should publish state msg to queue', (done) => {
      _plugin.setDeviceState('foo', 'bar')
        .then(() => {
          done()
        }).catch((err) => {
          done(err)
        })
    })
  })

  describe('#logging', () => {
    it('should send a log to logger queues', (done) => {
      _plugin.log('dummy log data')
        .then(() => {
          done()
        }).catch((err) => {
          done(err)
        })
    })

    it('should send an exception log to exception logger queues', (done) => {
      _plugin.logException(new Error('tests'))
        .then(() => {
          done()
        }).catch((err) => {
          done(err)
        })
    })
  })
})
