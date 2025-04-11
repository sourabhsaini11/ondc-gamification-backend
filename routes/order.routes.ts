import { Router } from "express"
import multer from "multer"
import orderController from "../controller/order/order.controller"
import { authenticate } from "../middleware/auth.middleware"

const orderRouter = Router()
const upload = multer({ dest: "uploads/" })
orderRouter.post("/upload-csv", authenticate, upload.single("file"), orderController.uploadCsv)
orderRouter.get("", orderController.getOrders)
orderRouter.get("/search2", orderController.search2)
orderRouter.get("/leaderboard", orderController.aggregatePointsSummary)
orderRouter.get("/create/leaderboard", orderController.createOrRefreshLeaderboardView)
orderRouter.get("/create/week-leaderboard", orderController.createOrRefreshWeeklyLeaderboardView)
orderRouter.get("/week-leaderboard", orderController.getWeeklyLeaderboardData)
orderRouter.get("/month-leaderboard", orderController.getMonthlyLeaderboardData)
orderRouter.get("/alltime-leaderboard", orderController.getAllTimeLeaderboardData)
orderRouter.get("/daily-leaderboard", orderController.getDailyLeaderboardData)
orderRouter.get("/daily-leaderboard2", orderController.getDailyLeaderboardData2)
orderRouter.get("/week-leaderboard2", orderController.getWeeklyLeaderboardData2)
orderRouter.get("/month-leaderboard2", orderController.getMonthlyLeaderboardData2)
orderRouter.get("/create/month-leaderboard", orderController.createOrRefreshMonthlyLeaderboardView)
orderRouter.get("/view/leaderboard", orderController.fetchLeaderboardData)
orderRouter.get("/uploads", authenticate, orderController.getUserUploads)
orderRouter.get("/download-csv", authenticate, orderController.downloadCSV)
orderRouter.get("/rewardledgertesting", orderController.rewardledgertesting)
orderRouter.get("/db", orderController.db)
orderRouter.get("/removetrigger", orderController.removetrigger)
orderRouter.get("/download-leaderboard", orderController.Downloadleaderboard)
export default orderRouter
