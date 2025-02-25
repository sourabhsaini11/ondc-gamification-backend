import { Request, Response } from "express"
import { parseAndStoreCsv, getOrders, getUserOrders } from "../../services"
import {
  aggregatePointsSummary,
  createOrRefreshLeaderboardView,
  createOrRefreshMonthlyLeaderboardView,
  createOrRefreshWeeklyLeaderboardView,
  fetchLeaderboardData,
  getDailyLeaderboardData,
  getMonthlyLeaderboardData,
  getWeeklyLeaderboardData,
  leaderboardTrigger,
} from "../../services/points.servce"
// import { logger } from "../../shared/logger"

const orderController = {
  uploadCsv: async (req: any, res: Response): Promise<Response> => {
    try {
      if (!req.file) {
        return res.status(400).json({ success: false, message: "No file uploaded" })
      }

      console.log("req", req)

      const filePath = req.file.path
      await parseAndStoreCsv(filePath, req.user?.userId)
      leaderboardTrigger()
      return res.status(200).json({ success: true, message: "CSV processed successfully" })
    } catch (error) {
      console.log(error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
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
      return res.status(500).json({ success: false, message: "Internal Server Error" })
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
