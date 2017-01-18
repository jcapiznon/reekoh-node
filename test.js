'use strict'

let Reekoh = require('./app')
<<<<<<< a8f6250e3b0f8aa288e09acd429a549ad672a581
let exceptionLogger = new Reekoh.plugins.ExceptionLogger()

/*
exceptionLogger.once('ready', () => {
  console.log(exceptionLogger.config)
  exceptionLogger.logException('asd')
}) */

exceptionLogger.on('exception', (logData) => {
  console.log(logData)
})

/*
// ready event
logger.once('ready', () => {
  console.log(logger.config)
  logger.logException('asd')
})
// log event
logger.on('log', (logData) => {
  console.log(logData)
})

*/
=======

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
>>>>>>> c8cfe4d56f6837e0d94131ae5522aded39ec9ce4
