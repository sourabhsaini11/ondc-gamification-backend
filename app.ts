import "dotenv/config"
import express, { Application, Request, Response, NextFunction } from "express"
import cors from "cors"
import HttpException, { sanitize } from "./shared/http-exception"
import locals from "./shared/locals.json"
import userRouter from "./routes/user.route"
import orderRouter from "./routes/order.routes"
import { aggregatePointsCron } from "./services/cron.service"

const createServer = (): express.Application => {
  const app: Application = express()
  app.options("*", cors())
  const corsOptions = {
    origin: "*", // Replace with your frontend domain
    methods: "GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS",
    allowedHeaders: "*",
    optionsSuccessStatus: 204,
  }

  // Use CORS middleware
  app.use(cors(corsOptions))

  app.use(express.json())
  app.use(express.urlencoded({ extended: true }))
  app.use("/api/v1/users", userRouter)
  app.use("/api/v1/orders", orderRouter)
  aggregatePointsCron()

  // eslint-disable-next-line no-unused-vars
  app.get("/", async (_req: Request, res: Response): Promise<Response> => {
    return res.status(200).send({
      success: true,
      message: "The server is running",
    })
  })

  // eslint-disable-next-line no-unused-vars
  app.get("/health", async (_req: Request, res: Response): Promise<Response> => {
    return res.status(200).send({
      success: true,
      message: "The server is running",
    })
  })

  // eslint-disable-next-line no-unused-vars, @typescript-eslint/no-unused-vars
  app.use((_req, _res) => {
    throw new HttpException(404, locals.notFound)
  })

  // eslint-disable-next-line no-unused-vars, @typescript-eslint/no-unused-vars
  app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
    err = sanitize(err)
    return res.status(err.status).json({ err: err.message })
  })
  return app
}

export default createServer
