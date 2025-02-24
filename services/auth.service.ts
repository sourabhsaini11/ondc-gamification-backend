import bcrypt from "bcrypt"
import jwt from "jsonwebtoken"
import { prisma } from "../prisma"

const SECRET_KEY = process.env.JWT_SECRET || "secret_key"

const authService = {
  async register(email: string, name: string, password: string) {
    // Check if user exists
    const existingUser = await prisma.user.findUnique({ where: { email } })
    if (existingUser) {
      throw new Error("User already exists with this email")
    }

    // Hash password
    const hashedPassword = await bcrypt.hash(password, 10)

    // Create user
    const newUser = await prisma.user.create({
      data: { email, name, password: hashedPassword },
    })

    return { id: newUser.id, email: newUser.email, name: newUser.name }
  },

  async login(email: string, password: string) {
    // Find user by email
    const user = await prisma.user.findUnique({ where: { email } })
    if (!user) {
      throw new Error("Invalid email or password")
    }

    // Compare passwords
    const isMatch = await bcrypt.compare(password, user.password)
    if (!isMatch) {
      throw new Error("Invalid email or password")
    }

    // Generate JWT token
    const token = jwt.sign({ userId: user.id, email: user.email }, SECRET_KEY, { expiresIn: "7d" })

    return { token, user: { id: user.id, email: user.email, name: user.name } }
  },
}

export default authService
