import express from "express"
import csvController from "../../controller/csvController"
import multer from "multer"

const router = express.Router()
const upload = multer({ dest: "uploads/" })
router.post("/upload-csv", upload.single("file"), csvController.uploadCsv)
router.get("/orders", csvController.getOrders)
router.get("/leaderboard", csvController.aggregatePointsSummary)
router.get("/create/leaderboard", csvController.createOrRefreshLeaderboardView)
router.get("/view/leaderboard", csvController.fetchLeaderboardData)

export default router
