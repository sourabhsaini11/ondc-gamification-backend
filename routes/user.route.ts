import { Router } from "express"
import userController from "../controller/user/user.controller"
const userRouter = Router()

userRouter.post("/register", userController.register)
userRouter.post("/login", userController.login)

export default userRouter
