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

const PLUGIN_ID = 'demo.gateway'
const COMMAND_RELAYS = 'demo.cmd.relays'
const OUTPUT_PIPES = 'demo.outpipe.1,demo.outpipe.2'
const BROKER = 'amqp://guest:guest@127.0.0.1/'

let rerelay = false

describe('Gateway Plugin Test', () => {
  before('#test init', () => {
    process.env.LOGGERS = ''
    process.env.CONFIG = '{}'
    process.env.EXCEPTION_LOGGERS = ''

    process.env.BROKER = BROKER
    process.env.PLUGIN_ID = PLUGIN_ID
    process.env.COMMAND_RELAYS = COMMAND_RELAYS
    process.env.OUTPUT_PIPES = OUTPUT_PIPES

    _broker = new Broker() // tester broker

    amqp.connect(process.env.BROKER).then((conn) => {
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

    it('should rcv `command` event', (done) => {
      let dummyData = { 'foo': 'bar' }

      _channel.sendToQueue(COMMAND_RELAYS, new Buffer(JSON.stringify(dummyData)))

      _plugin.on('command', (data) => {
        if (!isEqual(data, dummyData)) {
          if (!rerelay) done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })
  })

  describe('#RPC', () => {
    it('should connect to broker', (done) => {
      _broker.connect(BROKER).then(() => {
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

      _broker.createRPC('server', 'deviceinfo').then((queue) => {
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
        _plugin.requestDeviceInfo('', () => {}).then(() => {
          // noop!
        }).catch((err) => {
          if (!isEqual(err.message, 'Kindly specify a valid device identifier')) {
            done(new Error('Returned value not matched.'))
          } else {
            done()
          }
        })
      })

      it('should throw error if deviceId is not a string', (done) => {
        _plugin.requestDeviceInfo('', () => {}).then(() => {
          // noop!
        }).catch((err) => {
          if (!isEqual(err.message, 'Kindly specify a valid device identifier')) {
            done(new Error('Returned value not matched.'))
          } else {
            done()
          }
        })
      })

      it('should request device info', function (done) {
        this.timeout(3000)

        _plugin.requestDeviceInfo('123').then((ret) => {
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
      _plugin.pipe('', '').then(() => {
        done(new Error('Expecting rejection. check function test param.'))
      }).catch((err) => {
        if (!isEqual(err.message, 'Invalid data received. Data should be and Object and should not empty.')) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })

    it('should publish data to output pipes', (done) => { // no sequence id
      _plugin.pipe({foo: 'bar'}, '').then(() => {
        done()
      }).catch(done)
    })

    it('should publish data to sanitizer', (done) => {
      _plugin.pipe({foo: 'bar'}, 'seq123').then(() => {
        done()
      }).catch(done)
    })
  })

  describe('#relayCommand()', () => {
    it('should throw error if message is empty', (done) => {
      _plugin.relayCommand('', '', '').then(() => {
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
      _plugin.relayCommand('test', '', '').then(() => {
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
      rerelay = true
      _plugin.relayCommand('test', ['a'], ['b']).then(() => {
        done()
      }).catch((err) => {
        done(err)
      })
    })
  })

  describe('#sendCommandResponse()', () => {
    it('should throw error if commandId is empty', (done) => {
      _plugin.sendCommandResponse('', '').then(() => {
        done(new Error('Expecting rejection. check function test param.'))
      }).catch((err) => {
        if (!isEqual(err.message, 'Kindly specify the command id')) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })

    it('should throw error if response is empty', (done) => {
      _plugin.sendCommandResponse('foo', '').then(() => {
        done(new Error('Expecting rejection. check function test param.'))
      }).catch((err) => {
        if (!isEqual(err.message, 'Kindly specify the response')) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })

    it('should publish a message to `cmd.response` queue', (done) => {
      _plugin.sendCommandResponse('foo', 'bar').then(() => {
        done()
      }).catch(done)
    })
  })

  describe('#notifyConnection()', () => {
    it('should throw error if deviceId is empty', (done) => {
      _plugin.notifyConnection('').then(() => {
        done(new Error('Expecting rejection. check function test param.'))
      }).catch((err) => {
        if (!isEqual(err.message, 'Kindly specify a valid device identifier')) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })

    it('should throw error if deviceId is not a string', (done) => {
      _plugin.notifyConnection(123).then(() => {
        done(new Error('Expecting rejection. check function test param.'))
      }).catch((err) => {
        if (!isEqual(err.message, 'Kindly specify a valid device identifier')) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })

    it('should publish a message to device', (done) => {
      _plugin.notifyConnection('test').then(() => {
        done()
      }).catch(done)
    })
  })

  describe('#notifyDisconnection()', () => {
    it('should throw error if deviceId is empty', (done) => {
      _plugin.notifyDisconnection('').then(() => {
        done(new Error('Expecting rejection. check function test param.'))
      }).catch((err) => {
        if (!isEqual(err.message, 'Kindly specify a valid device identifier')) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })

    it('should throw error if deviceId is not a string', (done) => {
      _plugin.notifyDisconnection(123).then(() => {
        done(new Error('Expecting rejection. check function test param.'))
      }).catch((err) => {
        if (!isEqual(err.message, 'Kindly specify a valid device identifier')) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })

    it('should publish a message to device', (done) => {
      _plugin.notifyDisconnection('test').then(() => {
        done()
      }).catch((err) => {
        done(err)
      })
    })
  })

  describe('#syncDevice()', () => {
    it('should throw error if deviceInfo is empty', (done) => {
      _plugin.syncDevice('', []).then(() => {
        done(new Error('Expecting rejection. check function test param.'))
      }).catch((err) => {
        if (!isEqual(err.message, 'Kindly specify a valid device information/details')) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })

    it('should throw error if deviceInfo is not a valid object', (done) => {
      _plugin.syncDevice('{}', []).then(() => {
        done(new Error('Expecting rejection. check function test param.'))
      }).catch((err) => {
        if (!isEqual(err.message, 'Kindly specify a valid device information/details')) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })

    it('should throw error if deviceInfo doesnt have `_id` or `id` property', (done) => {
      _plugin.syncDevice({}, []).then(() => {
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
      _plugin.syncDevice({_id: 123}, []).then(() => {
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
      _plugin.syncDevice({_id: 123, name: 'foo'}, []).then(() => {
        done()
      }).catch(done)
    })
  })

  describe('#removeDevice()', () => {
    it('should throw error if deviceId is empty', (done) => {
      _plugin.removeDevice('').then(() => {
        done(new Error('Expecting rejection. check function test param.'))
      }).catch((err) => {
        if (!isEqual(err.message, 'Kindly specify a valid device identifier')) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })

    it('should throw error if deviceId is not a string', (done) => {
      _plugin.removeDevice(123).then(() => {
        done(new Error('Expecting rejection. check function test param.'))
      }).catch((err) => {
        if (!isEqual(err.message, 'Kindly specify a valid device identifier')) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })

    it('should publish remove msg to queue', (done) => {
      _plugin.removeDevice('test').then(() => {
        done()
      }).catch(done)
    })
  })

  describe('#setDeviceState()', () => {
    it('should throw error if deviceId is empty', (done) => {
      _plugin.setDeviceState('', '').then(() => {
        done(new Error('Expecting rejection. check function test param.'))
      }).catch((err) => {
        if (!isEqual(err.message, 'Kindly specify a valid device identifier')) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })

    it('should throw error if deviceId is not a string', (done) => {
      _plugin.setDeviceState(123, '').then(() => {
        done(new Error('Expecting rejection. check function test param.'))
      }).catch((err) => {
        if (!isEqual(err.message, 'Kindly specify a valid device identifier')) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })

    it('should throw error if state is empty', (done) => {
      _plugin.setDeviceState('test', '').then(() => {
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
      _plugin.setDeviceState('foo', 'bar').then(() => {
        done()
      }).catch(done)
    })
  })

  describe('#logging', () => {
    it('should send a log to logger queues', (done) => {
      _plugin.log('dummy log data').then(() => {
        done()
      }).catch(done)
    })

    it('should send an exception log to exception logger queues', (done) => {
      _plugin.logException(new Error('test err msg')).then(() => {
        done()
      }).catch(done)
    })
  })
})
