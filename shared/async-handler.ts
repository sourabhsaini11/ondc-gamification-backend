import { Response } from "express"

// eslint-disable-next-line no-unused-vars
const asyncUtil = (fn: (...args: any[]) => Promise<void | Response>) =>
  function asyncUtilWrap(...args: any[]): Promise<void | Response> {
    const fnReturn = fn(...args)
    const next = args[args.length - 1]
    return Promise.resolve(fnReturn).catch(next)
  }

export = asyncUtil
