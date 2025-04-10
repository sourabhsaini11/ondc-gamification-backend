import { Request, Response } from "express"
import {
  parseAndStoreCsv,
  getUserOrders,
  getUserOrdersForCSV,
  aggregateDailyGmvAndPoints,
  search,
  rewardledgertesting,
  db,
  removetrigger,
  search2,
  downloadleaderboard,
} from "../../services"
import {
  aggregatePointsSummary,
  createOrRefreshLeaderboardView,
  createOrRefreshMonthlyLeaderboardView,
  createOrRefreshWeeklyLeaderboardView,
  fetchLeaderboardData,
  fetchLeaderboardForWeek,
  fetchLeaderboardForWeek2,
  getAllTimeLeaders,
  getDailyLeaderboardData,
  getLeaderboardByDate,
  getLeaderboardByDate2,
  getMonthlyLeaderboardData,
  getMonthlyLeaderboardData2,
  getWeeklyLeaderboardData,
  DayWinnerUpdate,
} from "../../services/points.servce"
import { Parser } from "json2csv"
import { logger } from "../../shared/logger"

const orderController = {
  uploadCsv: async (req: any, res: Response): Promise<Response> => {
    try {
      if (!req.file) {
        return res.status(400).json({ success: false, message: "No file uploaded" })
      }

      const filePath = req.file.path

      const name = req.user?.email.split("@")[0]
      logger.info("name", name)
      const result = await parseAndStoreCsv(filePath, req.user?.userId, name)

      if (!result.success) {
        return res.status(400).json({ success: false, message: result.message })
      }

      return res.status(200).json({ success: true, message: result.message })
    } catch (error: any) {
      return res.status(500).json({ success: false, message: error.message || "Internal Server Error" })
    }
  },

  downloadCSV: async (req: any, res: Response): Promise<void> => {
    try {
      const userId = req.user?.userId
      const { orders } = await getUserOrdersForCSV(userId)

      if (!orders || orders.length === 0) {
        res.status(404).json({ success: false, message: "No orders found." })
        return
      }

      const fields = [
        "game_id",
        "order_id",
        "total_price",
        "order_status",
        "points",
        "timestamp_created",
        "timestamp_updated",
      ]
      const opts = { fields }
      const parser = new Parser(opts)
      const csv = parser?.parse(orders)

      res.setHeader("Content-Type", "text/csv")
      res.setHeader("Content-Disposition", "attachment; filename=orders.csv")
      res.status(200).send(csv)
    } catch (error) {
      logger.error("Error generating CSV:", error)
      res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  // eslint-disable-next-line no-unused-vars
  getOrders: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const orders = await aggregateDailyGmvAndPoints()
      logger.info("orders", JSON.stringify(orders))
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },
  // eslint-disable-next-line no-unused-vars
  db: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const ledger = await db()
      logger.info("ledger", ledger)
      return res.status(200).json({ success: true, data: ledger })
    } catch (err) {
      logger.info(err)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },
  // eslint-disable-next-line no-unused-vars
  rewardledgertesting: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const ledger = await rewardledgertesting()
      logger.info("ledger", ledger)
      return res.status(200).json({ success: true, data: ledger })
    } catch (err) {
      logger.info(err)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },
  // eslint-disable-next-line no-unused-vars
  removetrigger: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const ledger = await removetrigger()
      logger.info("ledger", ledger)
      return res.status(200).json({ success: true, data: ledger })
    } catch (err) {
      logger.info(err)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  search: async (req: Request, res: Response): Promise<Response> => {
    try {
      logger.info("Request Query Params:", req.query)

      // Extract & validate query params
      const { format, game_id } = req.query

      if (!format || !game_id) {
        return res.status(400).json({ success: false, message: "Missing required parameters: format and game_id" })
      }

      if (typeof format !== "string" || typeof game_id !== "string") {
        return res.status(400).json({ success: false, message: "Invalid parameter types" })
      }

      const Points = await search(game_id, format)

      logger.info("Points:", JSON.stringify(Points))

      return res.status(200).json({ success: true, data: Points })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  search2: async (req: Request, res: Response): Promise<Response> => {
    try {
      logger.info("Request Query Params:", req.query)

      // Extract & validate query params
      const { format, game_id } = req.query

      if (!format || !game_id) {
        return res.status(400).json({ success: false, message: "Missing required parameters: format and game_id" })
      }

      if (typeof format !== "string" || typeof game_id !== "string") {
        return res.status(400).json({ success: false, message: "Invalid parameter types" })
      }

      const Points = await search2(game_id, format)

      logger.info("Points:", JSON.stringify(Points))

      return res.status(200).json({ success: true, data: Points })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  aggregatePointsSummary: async (_req: Request, res: Response): Promise<Response> => {
    try {
      logger.info("_req", _req.body)
      const orders = await aggregatePointsSummary()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },
  // eslint-disable-next-line no-unused-vars
  createOrRefreshLeaderboardView: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const orders = await createOrRefreshLeaderboardView()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },
  // eslint-disable-next-line no-unused-vars
  createOrRefreshWeeklyLeaderboardView: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const orders = await createOrRefreshWeeklyLeaderboardView()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  getWeeklyLeaderboardData: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const { date } = _req.query

      const orders = date ? await fetchLeaderboardForWeek(date as any) : await getWeeklyLeaderboardData()

      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  getWeeklyLeaderboardData2: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const { date } = _req.query

      const orders = date ? await fetchLeaderboardForWeek2(date as any) : await getWeeklyLeaderboardData()

      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },
  // eslint-disable-next-line no-unused-vars
  createOrRefreshMonthlyLeaderboardView: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const orders = await createOrRefreshMonthlyLeaderboardView()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error", data: [] })
    }
  },

  getDailyLeaderboardData: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const { date } = _req.query
      const orders = date ? await getLeaderboardByDate(date as any) : await getDailyLeaderboardData()

      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },
  // eslint-disable-next-line no-unused-vars
  getMonthlyLeaderboardData: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const orders = await getMonthlyLeaderboardData()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  getDailyLeaderboardData2: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const { date } = _req.query
      const orders = date ? await getLeaderboardByDate2(date as any) : await getDailyLeaderboardData()

      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },
  // eslint-disable-next-line no-unused-vars
  getMonthlyLeaderboardData2: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const orders = await getMonthlyLeaderboardData2()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },
  // eslint-disable-next-line no-unused-vars
  fetchLeaderboardData: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const orders = await fetchLeaderboardData()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  getUserUploads: async (req: any, res: Response): Promise<Response> => {
    try {
      const userId = req.user?.userId
      const page = parseInt(req.query.page as string) || 1
      const limit = parseInt(req.query.limit as string) || 10

      if (page < 1 || limit < 1) {
        return res.status(400).json({ success: false, message: "Invalid pagination values" })
      }

      const { orders, totalOrders }: any = await getUserOrders(userId, page, limit)

      return res.status(200).json({
        success: true,
        data: orders,
        pagination: {
          totalOrders,
          currentPage: page,
          totalPages: Math.ceil(totalOrders / limit),
        },
      })
    } catch (error) {
      logger.info(error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },
  // eslint-disable-next-line no-unused-vars
  getAllTimeLeaderboardData: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const orders = await getAllTimeLeaders()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },
  // eslint-disable-next-line no-unused-vars
  DayWinnerUpdate: async (_req: Request, res: Response): Promise<Response> => {
    try {
      logger.info("DayWinnerUpdate")
      const orders = await DayWinnerUpdate()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },
  // eslint-disable-next-line no-unused-vars
  Downloadleaderboard: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const choice = _req.query
      logger.info("leaderboard format selected", choice)
      let type
      switch (choice as unknown as string) {
        case "daily":
          type = "daily_top_leaderboard"
          break
        case "monthly":
          type = "monthly_top_leaderboard"
          break
        default:
          type = "weekly_top_leaderboard"
      }

      const leaderboard = await downloadleaderboard(type)
      return res.status(200).json({ success: true, data: leaderboard })
    } catch (error) {
      logger.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },
}

export default orderController
