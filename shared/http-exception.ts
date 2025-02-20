import locals from "./locals.json"
import { logger } from "./logger"

export default class HttpException extends Error {
  status: number = 500

  message: string = locals.errorInternal

  constructor(status: number = 500, message: string = locals.errorInternal) {
    super()

    this.status = status

    this.message = message
  }
}

export function sanitize(error: any): HttpException {
  switch (true) {
    case error.status === 404:
      return error

    case error instanceof HttpException:
      return error

    default:
      logger.log("error", error)

      return new HttpException(500, error.message)
  }
}
