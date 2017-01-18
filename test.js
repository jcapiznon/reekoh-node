'use strict'

let Reekoh = require('./app')

let stream = new Reekoh.plugins.Stream()

stream.once('ready', () => {
  console.log(stream.config)
  stream.pipe({'temp': 300}, 'sQ1')
  stream.notifyConnection('device1')
  stream.notifyDisconnection('device1')
  stream.setDeviceState('device1', 'shit')
  stream.log('dummy log data')
  stream.logException(new Error('test exception'))
  stream.requestDeviceInfo('device1')
    .then(() => {
      console.log('yeah')
    })
    .catch((error) => {
      console.log(error)
    })
})

stream.on('message', (message) => {
  console.log(message)
})