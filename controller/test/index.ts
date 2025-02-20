import { Response, Request } from "express"
import { logger } from "../../shared/logger"
import wrap from "../../shared/async-handler"

const controller = {
  testFeature: async (req: Request, res: Response): Promise<void | Response> => {
    logger.info(req)
    return res.status(200).send({ success: true })
  },
  // Remove this method, this is for testing purposes only
  // eslint-disable-next-line no-unused-vars, @typescript-eslint/no-unused-vars
  testError: wrap(async (_req: Request, _res: Response): Promise<void | Response> => {
    throw new ReferenceError()
  }),
}

export default controller
