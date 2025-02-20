import express from "express"
import testController from "../../controller/test"

const router = express.Router()
router.post("/test-feature", testController.testFeature)
router.get("/test-error", testController.testError)

export default router
