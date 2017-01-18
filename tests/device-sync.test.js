/* global describe, it, before, after */

'use strict'

let _conn = null
let _plugin = null
let _channel = null

const amqp = require('amqplib')
const reekoh = require('../app.js')
const isEmpty = require('lodash.isempty')
const isEqual = require('lodash.isequal')

const ERR_RETURN_UNMATCH = 'Returned value not matched.'
const ERR_EMPTY_LOG_DATA = 'Kindly specify the data to log'
const ERR_NOT_ERRINSTANCE = 'Kindly specify a valid error to log'
const ERR_EXPECT_REJECTION = 'Expecting rejection. Check your test data.'

const ERR_RCVD_EMPTY_DEVICE_INFO = 'Received empty device info'
const ERR_EMPTY_IDENTIFIER = 'Kindly specify the device identifier'
const ERR_EMPTY_DEVICE_ID = 'Kindly specify a valid id for the device'
const ERR_EMPTY_DEVICE_NAME = 'Kindly specify a valid name for the device'
const ERR_EMPTY_DEVICE_INFO = 'Kindly specify the device information/details'

describe('DeviceSync Test', () => {
  before('#test init', () => {
    process.env.LOGGERS = ''
    process.env.EXCEPTION_LOGGERS = ''
    process.env.PLUGIN_ID = 'demo.dev-sync'
    process.env.BROKER = 'amqp://guest:guest@127.0.0.1/'

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
        _plugin = new reekoh.plugins.DeviceSync()
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

    it('should rcv `sync` device event', (done) => {
      let dummyData = {'operation': 'sync', '_id': 123, 'name': 'device-123'}
      _channel.sendToQueue(process.env.PLUGIN_ID, new Buffer(JSON.stringify(dummyData)))

      _plugin.on('sync', () => {
        done()
      })
    })

    it('should rcv `adddevice` event', (done) => {
      let data = {'operation': 'adddevice', 'device': {'_id': 123, 'name': 'device-123'}}
      _channel.sendToQueue(process.env.PLUGIN_ID, new Buffer(JSON.stringify(data)))

      _plugin.on('adddevice', (device) => {
        if (isEmpty(device)) {
          done(new Error(ERR_RCVD_EMPTY_DEVICE_INFO))
        } else {
          done()
        }
      })
    })

    it('should rcv `updatedevice` event', (done) => {
      let data = {'operation': 'updatedevice', 'device': {'_id': 123, 'name': 'device-123'}}
      _channel.sendToQueue(process.env.PLUGIN_ID, new Buffer(JSON.stringify(data)))

      _plugin.on('updatedevice', (device) => {
        if (isEmpty(device)) {
          done(new Error(ERR_RCVD_EMPTY_DEVICE_INFO))
        } else {
          done()
        }
      })
    })

    it('should rcv `removedevice` event', (done) => {
      let data = {'operation': 'removedevice', 'device': {'_id': 123, 'name': 'device-123'}}
      _channel.sendToQueue(process.env.PLUGIN_ID, new Buffer(JSON.stringify(data)))

      _plugin.on('removedevice', (device) => {
        if (isEmpty(device)) {
          done(new Error(ERR_RCVD_EMPTY_DEVICE_INFO))
        } else {
          done()
        }
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
      _plugin.syncDevice({foo: 'bar'}, [])
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

  describe('#remove()', () => {
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

  describe('#logging', () => {
    describe('.log()', () => {
      it('should throw error if logData is empty', (done) => {
        _plugin.log('')
          .then(() => {
            done(new Error(ERR_EXPECT_REJECTION))
          }).catch((err) => {
            if (!isEqual(err.message, ERR_EMPTY_LOG_DATA)) {
              done(new Error(ERR_RETURN_UNMATCH))
            } else {
              done()
            }
          })
      })

      it('should send a log to logger queues', (done) => {
        _plugin.log('dummy log data')
          .then(() => {
            done()
          }).catch((err) => {
            done(err)
          })
      })
    })

    describe('.logException()', () => {
      it('should throw error if param is not an Error instance', (done) => {
        _plugin.logException('')
          .then(() => {
            done(new Error(ERR_EXPECT_REJECTION))
          }).catch((err) => {
            if (!isEqual(err.message, ERR_NOT_ERRINSTANCE)) {
              done(new Error(ERR_RETURN_UNMATCH))
            } else {
              done()
            }
          })
      })
      it('should send an exception log to exception logger queues', (done) => {
        _plugin.logException(new Error('test'))
          .then(() => {
            done()
          }).catch((err) => {
            done(err)
          })
      })
    })
  })
})
