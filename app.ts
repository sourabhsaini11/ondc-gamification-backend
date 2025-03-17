import "dotenv/config"
import express, { Application, Request, Response, NextFunction } from "express"
// import helmet from "helmet"
import cors from "cors"
// import pg from "pg"
import HttpException, { sanitize } from "./shared/http-exception"
import locals from "./shared/locals.json"
import userRouter from "./routes/user.route"
import orderRouter from "./routes/order.routes"
import { aggregatePointsCron } from "./services/cron.service"
// import { leaderboardTrigger } from "./services/points.servce"

const createServer = (): express.Application => {
  const app: Application = express()
  // app.use(helmet({ crossOriginResourcePolicy: false }))
  app.options("*", cors())
  const corsOptions = {
    origin: "*", // Replace with your frontend domain
    methods: "GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS",
    allowedHeaders: "*",
    optionsSuccessStatus: 204,
  }

  // Use CORS middleware
  app.use(cors(corsOptions))
  // app.use(cors())
  app.use(express.json())
  app.use(express.urlencoded({ extended: true }))
  app.use("/api/v1/users", userRouter)
  app.use("/api/v1/orders", orderRouter)
  aggregatePointsCron()
  // leaderboardTrigger()
  //   .then(() => console.log("✅ Leaderboard trigger initialized"))
  //   .catch((err) => console.error("❌ Error initializing trigger:", err))
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

  // const { Client } = pg
  // const client = new Client({
  //   user: "postgres",
  //   host: "localhost",
  //   database: "gamafication",
  //   password: "postgres",
  //   port: 5432,
  // })

  // client
  //   .connect()
  //   .then(() => console.log("Connected to PostgreSQL"))
  //   .catch((err: any) => console.error("Connection error", err))

  // eslint-disable-next-line no-unused-vars, @typescript-eslint/no-unused-vars
  app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
    err = sanitize(err)
    return res.status(err.status).json({ err: err.message })
  })
  return app
}

export default createServer
