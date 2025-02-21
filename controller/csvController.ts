import { Request, Response } from "express"
import { parseAndStoreCsv, getOrders } from "../services/csvService"
// import { logger } from "../../shared/logger"

const csvController = {
  uploadCsv: async (req: Request, res: Response): Promise<Response> => {
    try {
      if (!req.file) {
        return res.status(400).json({ success: false, message: "No file uploaded" })
      }

      const filePath = req.file.path
      await parseAndStoreCsv(filePath)

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
      return res.status(200).json({ success: true, data: orders })
    } catch (error) {
      console.error("❌ Error retrieving orders:", error)
      return res.status(500).json({ success: false, message: "Internal Server Error" })
    }
  },
}

export default csvController
