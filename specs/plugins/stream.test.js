'use strict'

const async = require('async')
const amqp = require('amqplib')
const Reekoh = require('../../index.js')
const isEqual = require('lodash.isequal')
const Broker = require('../../lib/broker.lib')

describe('Stream Plugin Test', () => {
  let _broker = new Broker()
  let _plugin = null
  let _channel = null
  let _conn = null

  let errLog = (err) => { console.log(err) }

  before('#test init', () => {
    process.env.PLUGIN_ID = 'plugin1'
    process.env.PIPELINE = 'Pl1'
    process.env.OUTPUT_PIPES = 'Op1,Op2'
    process.env.LOGGERS = 'logger1,logger2'
    process.env.EXCEPTION_LOGGERS = 'exlogger1,exlogger2'
    process.env.BROKER = 'amqp://guest:guest@127.0.0.1/'
    process.env.CONFIG = '{"foo": "bar"}'
    process.env.OUTPUT_SCHEME = 'MERGE'
    process.env.OUTPUT_NAMESPACE = 'result'
    process.env.ACCOUNT = 'demo account'

    amqp.connect(process.env.BROKER)
      .then((conn) => {
        _conn = conn
        return conn.createChannel()
      }).then((channel) => {
        _channel = channel
      }).catch(errLog)
  })

  after('#terminate connection', (done) => {
    _conn.close()
      .then(() => {
        _plugin.removeAllListeners()
        done()
      })
  })

  describe('#spawn', () => {
    it('should spawn the class without error', (done) => {
      _plugin = new Reekoh.plugins.Stream()
      _plugin.once('ready', () => {
        done()
      })
    })
  })

  describe('#events', () => {
    it('should receive `message` event', (done) => {
      let dummyData = { 'foo': 'bar' }
      _channel.sendToQueue(process.env.PIPELINE, new Buffer(JSON.stringify(dummyData)))

      _plugin.on('message', (data) => {
        if (!isEqual(data, dummyData)) {
          done(new Error('received data not matched'))
        } else {
          done()
        }
      })
    })

    it('should receive `sync` event', (done) => {
      let dummyData = { 'foo': 'bar' }
      _channel.sendToQueue(process.env.PLUGIN_ID, new Buffer(JSON.stringify(dummyData)))

      _plugin.on('sync', () => {
        done()
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
          resolve(JSON.stringify(msg.content))
        })
      }

      _broker.newRpc('server', 'deviceinfo')
        .then((queue) => {
          return queue.serverConsume(sampleServerProcedure)
        }).then(() => {
        // Awaiting RPC requests
          done()
        }).catch((err) => {
          done(err)
        })
    })
  })

  describe('.requestDeviceInfo()', function () {
    this.timeout(8000)
    it('should throw error if deviceId is empty', (done) => {
      _plugin.requestDeviceInfo('')
        .then(() => {
          // noop!
        }).catch((err) => {
          if (!isEqual(err, new Error('Please specify the device identifier.'))) {
            done(new Error('Return value did not match.'))
          } else {
            done()
          }
        })
    })

    it('should request device info', (done) => {
      _plugin.requestDeviceInfo('device1')
        .then((reply) => {
          console.log(reply)
          done()
        }).catch((err) => {
          done(err)
        })
    })
  })

  describe('#pipe()', () => {
    it('should throw error if data is empty', (done) => {
      _plugin.pipe('', '')
        .then(() => {
          done(new Error('Reject expected.'))
        }).catch((err) => {
          if (!isEqual(err, new Error('Invalid data received. Data should be and Object and not empty.'))) {
            done(new Error('Return value did not match.'))
          } else {
            done()
          }
        })
    })

    it('should publish data to output pipes', (done) => {
      _plugin.pipe({foo: 'bar'}, '') // no seqs
        .then(() => {
          done()
        }).catch((err) => {
          done(err)
        })
    })

    it('should publish data to sanitizer', (done) => {
      _plugin.pipe({foo: 'bar'}, 'seq123')
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
          done(new Error('Reject expected.'))
        }).catch((err) => {
          if (!isEqual(new Error('Please specify the device identifier.'), err)) {
            done(new Error('Return value did not match.'))
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
          done(new Error('Reject expected.'))
        }).catch((err) => {
          if (!isEqual(new Error('Please specify the device identifier.'), err)) {
            done(new Error('Return value did not match.'))
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

  describe('#setDeviceState()', () => {
    it('should throw error if deviceId is empty', (done) => {
      _plugin.setDeviceState('', '')
        .then(() => {
          done(new Error('Reject expected.'))
        }).catch((err) => {
          if (!isEqual(new Error('Please specify the device identifier.'), err)) {
            done(new Error('Return value did not match.'))
          } else {
            done()
          }
        })
    })

    it('should throw error if state is empty', (done) => {
      _plugin.setDeviceState('test', '')
        .then(() => {
          done(new Error('Reject expected.'))
        }).catch((err) => {
          if (!isEqual(err, new Error('Please specify the device state.'))) {
            done(new Error('Return value did not match.'))
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
        }).catch(() => {
          done(new Error('send using logger fail.'))
        })
    })

    it('should send an exception log to exception logger queues', (done) => {
      _plugin.logException(new Error('tests'))
        .then(() => {
          done()
        }).catch(() => {
          done(new Error('send using exception logger fail.'))
        })
    })
  })
})
