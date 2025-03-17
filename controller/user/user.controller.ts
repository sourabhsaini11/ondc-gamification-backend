import { Request, Response } from "express"
import authService from "../../services/auth.service"

const userController = {
  async register(req: Request, res: Response) {
    const { email, name, password } = req.body
    try {
      const user = await authService.register(email, name, password)
      return res.status(201).json({ message: "User registered successfully", user })
    } catch (error: any) {
      return res.status(400).json({ error: error.message })
    }
  },

  async login(req: Request, res: Response) {
    const { email, password } = req.body
    try {
      const data = await authService.login(email, password)
      return res.status(200).json({ message: "Login successful", ...data })
    } catch (error: any) {
      console.log("error", error)
      return res.status(400).json({ error: error.message })
    }
  },
}
export default userController
