import { Router } from "express"
import multer from "multer"
import orderController from "../controller/order/order.controller"
import { authenticate } from "../middleware/auth.middleware"

const orderRouter = Router()
const upload = multer({ dest: "uploads/" })
orderRouter.post("/upload-csv", authenticate, upload.single("file"), orderController.uploadCsv)
orderRouter.get("", orderController.getOrders)
orderRouter.get("/leaderboard", orderController.aggregatePointsSummary)
orderRouter.get("/create/leaderboard", orderController.createOrRefreshLeaderboardView)
orderRouter.get("/create/week-leaderboard", orderController.createOrRefreshWeeklyLeaderboardView)
orderRouter.get("/week-leaderboard", orderController.getWeeklyLeaderboardData)
orderRouter.get("/month-leaderboard", orderController.getMonthlyLeaderboardData)
orderRouter.get('/alltime-leaderboard', orderController.getAllTimeLeaderboardData)
orderRouter.get("/daily-leaderboard", orderController.getDailyLeaderboardData)
orderRouter.get("/create/month-leaderboard", orderController.createOrRefreshMonthlyLeaderboardView)
orderRouter.get("/view/leaderboard", orderController.fetchLeaderboardData)
orderRouter.get("/uploads", authenticate, orderController.getUserUploads)
orderRouter.get("/download-csv", authenticate, orderController.downloadCSV)

export default orderRouter
