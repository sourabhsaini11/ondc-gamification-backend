import { Request, Response } from "express"
import {
  parseAndStoreCsv,
  getUserOrders,
  // getOrders,
  rewardledger,
  getUserOrdersForCSV,
  aggregateDailyGmvAndPoints,
  search,
} from "../../services"
import {
  aggregatePointsSummary,
  createOrRefreshLeaderboardView,
  createOrRefreshMonthlyLeaderboardView,
  createOrRefreshWeeklyLeaderboardView,
  fetchLeaderboardData,
  fetchLeaderboardForWeek,
  getAllTimeLeaders,
  getDailyLeaderboardData,
  getLeaderboardByDate,
  getMonthlyLeaderboardData,
  getWeeklyLeaderboardData,
  // leaderboardTrigger,
  rewardLedgerTrigger,
  DayWinnerUpdate,
  PointsAssignedforhighestGmv
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
        "name",
        // "domain",
        "buyer_app_id",
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
      const ledger =  await rewardledger()
      console.log("ledger", ledger)
      return res.status(200).json({ success: true, data: ledger })
    } catch (err) {
      console.log(err)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },

  search: async (req: Request, res: Response): Promise<Response> => {
    try {
      console.log("Request Query Params:", req.query);

      // Extract & validate query params
      const { format, game_id } = req.query;

      if (!format || !game_id) {
        return res.status(400).json({ success: false, message: "Missing required parameters: format and game_id" });
      }

      if (typeof format !== "string" || typeof game_id !== "string") {
        return res.status(400).json({ success: false, message: "Invalid parameter types" });
      }

      const Points = await search(game_id, format);

      console.log("Points:", JSON.stringify(Points));

      return res.status(200).json({ success: true, data: Points });
    } catch (error) {
      console.error("❌ Error retrieving orders:", error);
      return res.status(500).json({ success: false, message: "Internal Server Error" });
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
  highestGmvandOrder: async (_req: Request, res: Response): Promise<Response> => {
    try {
      console.log("_req", _req)
      console.log("highestGmvandOrder")
      const orders = await PointsAssignedforhighestGmv()
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
  }
}

export default orderController
