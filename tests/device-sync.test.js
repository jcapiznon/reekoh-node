'use strict'

let amqp = require('amqplib')
let reekoh = require('../app.js')
let isEqual = require('lodash.isequal')
let isEmpty = require('lodash.isempty')

describe('DeviceSync Test', () => {
  // --- preparation
  process.env.LOGGERS = ''
  process.env.EXCEPTION_LOGGERS = ''

  process.env.PLUGIN_ID = 'demo.dev-sync'
  process.env.BROKER = 'amqp://guest:guest@127.0.0.1/'

  let _channel = null
  let _plugin = new reekoh.plugins.DeviceSync()

  let errLog = (err) => { console.log(err) }

  amqp.connect(process.env.BROKER)
    .then((conn) => {
      return conn.createChannel()
    }).then((channel) => {
      _channel = channel
    }).catch(errLog)

  // --- tests

  describe('#spawn', () => {
    it('should spawn the class without error', (done) => {
      _plugin.once('ready', () => {
        done()
      })
    })
  })

  describe('#events', () => {
    it('should rcv sync device event', (done) => {
      let dummyData = {'operation': 'sync', '_id': 123, 'name': 'device-123'}
      _channel.sendToQueue(process.env.PLUGIN_ID, new Buffer(JSON.stringify(dummyData)))

      _plugin.on('sync', () => {
        done()
      })
    })

    it('should rcv add device event', (done) => {
      let data = {'operation': 'adddevice', 'device': {'_id': 123, 'name': 'device-123'}}
      _channel.sendToQueue(process.env.PLUGIN_ID, new Buffer(JSON.stringify(data)))

      _plugin.on('adddevice', (device) => {
        if (!isEmpty(device)) {
          done()
        } else {
          done(new Error('adding empty device info'))
        }
      })
    })

    it('should rcv update device event', (done) => {
      let data = {'operation': 'updatedevice', 'device': {'_id': 123, 'name': 'device-123'}}
      _channel.sendToQueue(process.env.PLUGIN_ID, new Buffer(JSON.stringify(data)))

      _plugin.on('updatedevice', (device) => {
        if (!isEmpty(device)) {
          done()
        } else {
          done(new Error('updating empty device info'))
        }
      })
    })

    it('should rcv remove device event', (done) => {
      let data = {'operation': 'removedevice', 'device': {'_id': 123, 'name': 'device-123'}}
      _channel.sendToQueue(process.env.PLUGIN_ID, new Buffer(JSON.stringify(data)))

      _plugin.on('removedevice', (device) => {
        if (!isEmpty(device)) {
          done()
        } else {
          done(new Error('removing empty device info'))
        }
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

  describe('#sync', () => {
    let data = { _id: 123, name: 'device123'}

    it('should sync a device to platfom', (done) => {
      _plugin.syncDevice(data).then(() => {
        done()
      }).catch(() => {
        done(new Error('sync fail.'))
      })
    })
  })

  describe('#remove', () => {
    it('should remove a device to platform', (doneRmv) => {
      _plugin.removeDevice(123)
        .then(() => {
          doneRmv()
        }).catch((err) => {
          console.log(err)
          doneRmv(new Error('send remove device to queue fail.'))
        })
    })
  })
})
