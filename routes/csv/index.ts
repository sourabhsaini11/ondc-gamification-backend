import express from "express"
import testController from "../../controller/test"
import csvController from "../../controller/csvController"
import multer from "multer"

const router = express.Router()
const upload = multer({ dest: "uploads/" })
router.post("/test-feature", testController.testFeature)
router.get("/test-error", testController.testError)
router.post("/upload-csv", upload.single("file"), csvController.uploadCsv)
router.get("/orders", csvController.getOrders)

export default router
