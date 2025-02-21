import userController from "../controller/user/user.controller";
import { Router } from "express";
const userRouter = Router()

userRouter.post('/login', userController.login)

export default userRouter