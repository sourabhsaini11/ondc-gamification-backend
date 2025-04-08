import { Request, Response } from "express"
import {
  parseAndStoreCsv,
  getUserOrders,
  // getOrders,
  rewardledger,
  getUserOrdersForCSV,
  aggregateDailyGmvAndPoints,
  search,
  rewardledgertesting,
  db,
  removetrigger,
  search2,
  downloadleaderboard
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
  // leaderboardTrigger,
  rewardLedgerTrigger,
  DayWinnerUpdate,
} from "../../services/points.servce"
// import { logger } from "../../shared/logger"
import { Parser } from "json2csv"

const orderController = {
  uploadCsv: async (req: any, res: Response): Promise<Response> => {
    try {
      if (!req.file) {
        return res.status(400).json({ success: false, message: "No file uploaded" })
      }

      const filePath = req.file.path
      console.log("req.uiserrrr",req.user?.userId)
      const result = await parseAndStoreCsv(filePath, req.user?.userId)

      if (!result.success) {
        return res.status(400).json({ success: false, message: result.message })
      }

      // leaderboardTrigger()
      rewardLedgerTrigger()
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
        // "domain",
        "total_price",
        // "shipping_charges",
        // "taxes",
        // "discount",
        // "convenience_fee",
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
      console.error("Error generating CSV:", error)
      res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  getOrders: async (_req: Request, res: Response): Promise<Response> => {
    try {
      console.log("_req", _req)
      const orders = await aggregateDailyGmvAndPoints()
      console.log("orders", JSON.stringify(orders))
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  rewardledger: async (_req: Request, res: Response): Promise<Response> => {
    try {
      console.log("_req", _req)
      const ledger = await rewardledger()
      console.log("ledger", ledger)
      return res.status(200).json({ success: true, data: ledger })
    } catch (err) {
      console.log(err)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  db: async (_req: Request, res: Response): Promise<Response> => {
    try {
      console.log("_req", _req)
      const ledger = await db()
      console.log("ledger", ledger)
      return res.status(200).json({ success: true, data: ledger })
    } catch (err) {
      console.log(err)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  rewardledgertesting: async (_req: Request, res: Response): Promise<Response> => {
    try {
      console.log("_req", _req)
      const ledger = await rewardledgertesting()
      console.log("ledger", ledger)
      return res.status(200).json({ success: true, data: ledger })
    } catch (err) {
      console.log(err)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  removetrigger: async (_req: Request, res: Response): Promise<Response> => {
    try {
      console.log("_req", _req)
      const ledger = await removetrigger()
      console.log("ledger", ledger)
      return res.status(200).json({ success: true, data: ledger })
    } catch (err) {
      console.log(err)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  search: async (req: Request, res: Response): Promise<Response> => {
    try {
      console.log("Request Query Params:", req.query)

      // Extract & validate query params
      const { format, game_id } = req.query

      if (!format || !game_id) {
        return res.status(400).json({ success: false, message: "Missing required parameters: format and game_id" })
      }

      if (typeof format !== "string" || typeof game_id !== "string") {
        return res.status(400).json({ success: false, message: "Invalid parameter types" })
      }

      const Points = await search(game_id, format)

      console.log("Points:", JSON.stringify(Points))

      return res.status(200).json({ success: true, data: Points })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  search2: async (req: Request, res: Response): Promise<Response> => {
    try {
      console.log("Request Query Params:", req.query)

      // Extract & validate query params
      const { format, game_id } = req.query

      if (!format || !game_id) {
        return res.status(400).json({ success: false, message: "Missing required parameters: format and game_id" })
      }

      if (typeof format !== "string" || typeof game_id !== "string") {
        return res.status(400).json({ success: false, message: "Invalid parameter types" })
      }

      const Points = await search2(game_id, format)

      console.log("Points:", JSON.stringify(Points))

      return res.status(200).json({ success: true, data: Points })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  aggregatePointsSummary: async (_req: Request, res: Response): Promise<Response> => {
    try {
      console.log("_req", _req.body)
      const orders = await aggregatePointsSummary()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  createOrRefreshLeaderboardView: async (_req: Request, res: Response): Promise<Response> => {
    try {
      console.log("_req", _req)
      const orders = await createOrRefreshLeaderboardView()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  createOrRefreshWeeklyLeaderboardView: async (_req: Request, res: Response): Promise<Response> => {
    try {
      console.log("_req", _req)
      const orders = await createOrRefreshWeeklyLeaderboardView()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  getWeeklyLeaderboardData: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const { date } = _req.query

      const orders = date ? await fetchLeaderboardForWeek(date as any) : await getWeeklyLeaderboardData()

      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  getWeeklyLeaderboardData2: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const { date } = _req.query

      const orders = date ? await fetchLeaderboardForWeek2(date as any) : await getWeeklyLeaderboardData()

      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  createOrRefreshMonthlyLeaderboardView: async (_req: Request, res: Response): Promise<Response> => {
    try {
      console.log("_req", _req)
      const orders = await createOrRefreshMonthlyLeaderboardView()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error", data: [] })
    }
  },

  getDailyLeaderboardData: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const { date } = _req.query
      const orders = date ? await getLeaderboardByDate(date as any) : await getDailyLeaderboardData()

      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  getMonthlyLeaderboardData: async (_req: Request, res: Response): Promise<Response> => {
    try {
      console.log("_req", _req)
      const orders = await getMonthlyLeaderboardData()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  getDailyLeaderboardData2: async (_req: Request, res: Response): Promise<Response> => {
    try {
      const { date } = _req.query
      const orders = date ? await getLeaderboardByDate2(date as any) : await getDailyLeaderboardData()

      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  getMonthlyLeaderboardData2: async (_req: Request, res: Response): Promise<Response> => {
    try {
      console.log("_req", _req)
      const orders = await getMonthlyLeaderboardData2()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  fetchLeaderboardData: async (_req: Request, res: Response): Promise<Response> => {
    try {
      console.log("_req", _req)
      const orders = await fetchLeaderboardData()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
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
      console.log(error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },
  getAllTimeLeaderboardData: async (_req: Request, res: Response): Promise<Response> => {
    try {
      console.log("_req", _req)
      const orders = await getAllTimeLeaders()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  DayWinnerUpdate: async (_req: Request, res: Response): Promise<Response> => {
    try {
      console.log("_req", _req)
      console.log("DayWinnerUpdate")
      const orders = await DayWinnerUpdate()
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },
  
  Downloadleaderboard: async (_req: Request, res: Response): Promise<Response> => {
    try {
     console.log(`Inside Download leaderboard ${_req}`)
      const choice = _req.query
      console.log("choide", choice)
      let type;
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
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  }
}

export default orderController
