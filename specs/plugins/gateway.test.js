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

const ENV_PIPELINE = 'demo.pipeline'
const ENV_BROKER = 'amqp://guest:guest@127.0.0.1/'

const QN_DEVICE_INFO = 'deviceinfo'

describe('Gateway Plugin Test', () => {
  before('#test init', () => {
    process.env.LOGGERS = ''
    process.env.EXCEPTION_LOGGERS = ''

    process.env.BROKER = ENV_BROKER
    process.env.PIPELINE = ENV_PIPELINE
    process.env.OUTPUT_PIPES = 'demo.outpipe.1,demo.outpipe.2'

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
      _channel.sendToQueue(ENV_PIPELINE, new Buffer(JSON.stringify(dummyData)))

      _plugin.on('message', (data) => {
        if (!isEqual(data, dummyData)) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })
  })

  describe('#RPC', () => {
    it('should connect to broker', (done) => {
      _broker.connect(ENV_BROKER)
        .then(() => {
          return done()
        }).catch((err) => {
          done(err)
        })
    })

    it('should spawn temporary RPC server', (done) => {
      // if request arrives this proc will be called
      let sampleServerProcedure = (msg) => {
        // console.log(msg.content.toString('utf8'))
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

      _broker.createRPC('server', QN_DEVICE_INFO)
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
            if (!isEqual(err.message, 'Kindly specify the device identifier')) {
              done(new Error('Returned value not matched.'))
            } else {
              done()
            }
          })
      })

      it('should request device info', function (done) {
        this.timeout(3000)

        _plugin.requestDeviceInfo('123')
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
          done(new Error('Expecting rejection. check function test param.'))
        }).catch((err) => {
          if (!isEqual(err.message, 'Kindly specify the data to forward')) {
            done(new Error('Returned value not matched.'))
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
          done(new Error('Expecting rejection. check function test param.'))
        }).catch((err) => {
          if (!isEqual(err.message, 'Kindly specify the command/message to send')) {
            done(new Error('Returned value not matched.'))
          } else {
            done()
          }
        })
    })

    it('should throw error if device or deviceTypes is empty', (done) => {
      _plugin.relayMessage('test', '', '')
        .then(() => {
          done(new Error('Expecting rejection. check function test param.'))
        }).catch((err) => {
          if (!isEqual(err.message, 'Kindly specify the target device types or devices')) {
            done(new Error('Returned value not matched.'))
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
          done(new Error('Expecting rejection. check function test param.'))
        }).catch((err) => {
          if (!isEqual(err.message, 'Kindly specify the device identifier')) {
            done(new Error('Returned value not matched.'))
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
          done(new Error('Expecting rejection. check function test param.'))
        }).catch((err) => {
          if (!isEqual(err.message, 'Kindly specify the device identifier')) {
            done(new Error('Returned value not matched.'))
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
          done(new Error('Expecting rejection. check function test param.'))
        }).catch((err) => {
          if (!isEqual(err.message, 'Kindly specify the device information/details')) {
            done(new Error('Returned value not matched.'))
          } else {
            done()
          }
        })
    })

    it('should throw error if deviceInfo doesnt have `_id` or `id` property', (done) => {
      _plugin.syncDevice({}, [])
        .then(() => {
          done(new Error('Expecting rejection. check function test param.'))
        }).catch((err) => {
          if (!isEqual(err.message, 'Kindly specify a valid id for the device')) {
            done(new Error('Returned value not matched.'))
          } else {
            done()
          }
        })
    })

    it('should throw error if deviceInfo doesnt have `name` property', (done) => {
      _plugin.syncDevice({_id: 123}, [])
        .then(() => {
          done(new Error('Expecting rejection. check function test param.'))
        }).catch((err) => {
          if (!isEqual(err.message, 'Kindly specify a valid name for the device')) {
            done(new Error('Returned value not matched.'))
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
          done(new Error('Expecting rejection. check function test param.'))
        }).catch((err) => {
          if (!isEqual(err.message, 'Kindly specify the device identifier')) {
            done(new Error('Returned value not matched.'))
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
          done(new Error('Expecting rejection. check function test param.'))
        }).catch((err) => {
          if (!isEqual(err.message, 'Kindly specify the device identifier')) {
            done(new Error('Returned value not matched.'))
          } else {
            done()
          }
        })
    })

    it('should throw error if state is empty', (done) => {
      _plugin.setDeviceState('test', '')
        .then(() => {
          done(new Error('Expecting rejection. check function test param.'))
        }).catch((err) => {
          if (!isEqual(err.message, 'Kindly specify the device state')) {
            done(new Error('Returned value not matched.'))
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
