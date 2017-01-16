'use strict'

let Reekoh = require('./app')
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
