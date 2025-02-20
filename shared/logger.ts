import winston, { Logger } from 'winston'
import 'dotenv/config'
import util from 'util'
const { combine, colorize } = winston.format

const transform = () => {
  return {
    transform: (info: any) => {
      info.oldMessage = info.message
      info.message = util.format(info.stack || info.message, ...(info[Symbol.for('splat')] || []))
      return info;
    },
  }
}

const transformBack = () => {
  return {
    transform: (info: any) => {
      info.message = info.oldMessage
      return info
    },
  }
}

const logger: Logger = winston.createLogger({
  format: combine(
    colorize(),
    transform(),
    winston.format.printf((info: any) => {
      return `${info.timestamp} [${info.level}] : ${info.stack || info.message}`
    }),
    transformBack(),
  ),
})

logger.add(
  new winston.transports.Console({
    level: 'debug',
  }),
)

export { logger }
