import { Router } from "express"
import multer from "multer"
import orderController from "../controller/order/order.controller"
import { authenticate } from "../middleware/auth.middleware"

const orderRouter = Router()
const upload = multer({ dest: " uploads/" })
//upload
orderRouter.post("/upload-csv", upload.single("file"), orderController.uploadCsv)
orderRouter.get("/uploads", authenticate, orderController.getUserUploads)
orderRouter.get("/search", orderController.search)
//leaderboard
orderRouter.get("/leaderboard", orderController.aggregatePointsSummary)
orderRouter.get("/alltime-leaderboard", orderController.getAllTimeLeaderboardData)
orderRouter.get("/daily-leaderboard", orderController.getDailyLeaderboardData)
orderRouter.get("/week-leaderboard", orderController.getWeeklyLeaderboardData)
orderRouter.get("/month-leaderboard", orderController.getMonthlyLeaderboardData)
orderRouter.get("/view/leaderboard", orderController.fetchLeaderboardData)
//download
orderRouter.get("/download-csv", authenticate, orderController.downloadCSV)
orderRouter.get("/download-leaderboard", orderController.Downloadleaderboard)
//test routes
orderRouter.get("/rewardledgertesting", orderController.rewardledgertesting)
orderRouter.get("/db", orderController.db)
orderRouter.get("/removetrigger", orderController.removetrigger)
orderRouter.get("/create/leaderboard", orderController.createOrRefreshLeaderboardView)
orderRouter.get("/create/week-leaderboard", orderController.createOrRefreshWeeklyLeaderboardView)
orderRouter.get("/create/month-leaderboard", orderController.createOrRefreshMonthlyLeaderboardView)
export default orderRouter
