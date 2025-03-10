import { Request, Response } from "express"
import { parseAndStoreCsv, getUserOrders, getOrders, getUserOrdersForCSV } from "../../services"
import {
  aggregatePointsSummary,
  createOrRefreshLeaderboardView,
  createOrRefreshMonthlyLeaderboardView,
  createOrRefreshWeeklyLeaderboardView,
  fetchLeaderboardData,
  getDailyLeaderboardData,
  getMonthlyLeaderboardData,
  getWeeklyLeaderboardData,
  // leaderboardTrigger,
  rewardLedgerTrigger,
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
        "domain",
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
      const orders = await getOrders()
      console.log("orders", JSON.stringify(orders))
      return res.status(200).json({ success: true, data: orders })
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
      console.log("_req", _req)
      const orders = await getWeeklyLeaderboardData()
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
      console.log("_req", _req)
      const orders = await getDailyLeaderboardData()
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
}

export default orderController
