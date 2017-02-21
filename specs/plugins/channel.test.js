/* global describe, it, before, after */

'use strict'

let _conn = null
let _plugin = null
let _channel = null

const amqp = require('amqplib')
const reekoh = require('../../index.js')
const isEqual = require('lodash.isequal')

// preserving.. plugin clears env after init
const PLUGIN_ID = 'demo.channel'
const INTPUT_PIPE = 'demo.channel.pipe'

describe('Channel Plugin Test', () => {
  before('#test init', () => {
    process.env.LOGGERS = ''
    process.env.EXCEPTION_LOGGERS = ''
    process.env.PLUGIN_ID = PLUGIN_ID
    process.env.INPUT_PIPE = INTPUT_PIPE
    process.env.BROKER = 'amqp://guest:guest@127.0.0.1/'

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
        _plugin = new reekoh.plugins.Channel()
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

    it('should rcv `data` event', function (done) {
      this.timeout(8000)

      let dummyData = { 'foo': 'bar' }
      _channel.sendToQueue(INTPUT_PIPE, new Buffer(JSON.stringify(dummyData)))

      _plugin.on('data', (data) => {
        if (!isEqual(data, dummyData)) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })
  })

  describe('#relayCommand()', () => {
    it('should throw error if message is empty', (done) => {
      _plugin.relayCommand('', '', '').then(() => {
        done(new Error('Expecting rejection. Check your test data.'))
      }).catch((err) => {
        if (!isEqual(err.message, 'Kindly specify a valid command/message to send')) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })

    it('should throw error if device and deviceTypes are empty', (done) => {
      _plugin.relayCommand('test', '', '').then(() => {
        done(new Error('Expecting rejection. Check your test data.'))
      }).catch((err) => {
        if (!isEqual(err.message, 'Kindly specify the target device types or devices')) {
          done(new Error('Returned value not matched.'))
        } else {
          done()
        }
      })
    })

    it('should publish a message to pipeline', (done) => {
      _plugin.relayCommand('test', ['a'], ['b']).then(() => {
        done()
      }).catch(done)
    })
  })

  describe('#logging', () => {
    describe('.log()', () => {
      it('should throw error if logData is empty', (done) => {
        _plugin.log('').then(() => {
          done(new Error('Expecting rejection. Check your test data.'))
        }).catch((err) => {
          if (!isEqual(err.message, 'Please specify a data to log.')) {
            done(new Error('Returned value not matched.'))
          } else {
            done()
          }
        })
      })

      it('should send a log to logger queues', (done) => {
        _plugin.log('dummy log data').then(() => {
          done()
        }).catch(done)
      })
    })

    describe('.logException()', () => {
      it('should throw error if param is not an Error instance', (done) => {
        _plugin.logException('').then(() => {
          done(new Error('Expecting rejection. Check your test data.'))
        }).catch((err) => {
          if (!isEqual(err.message, 'Please specify a valid error to log.')) {
            done(new Error('Returned value not matched.'))
          } else {
            done()
          }
        })
      })
      it('should send an exception log to exception logger queues', (done) => {
        _plugin.logException(new Error('test')).then(() => {
          done()
        }).catch(done)
      })
    })
  })
})
