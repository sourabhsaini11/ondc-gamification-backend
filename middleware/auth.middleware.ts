import { Request, Response, NextFunction } from "express"
import jwt from "jsonwebtoken"

const SECRET_KEY = process.env.JWT_SECRET || "secret_key"

export interface AuthenticatedRequest extends Request {
  user?: any
}

export const authenticate = (req: AuthenticatedRequest, res: Response, next: NextFunction): Response | void => {
  const token = req.headers.authorization?.split(" ")[1]

  if (!token) {
    return res.status(401).json({ message: "Unauthorized - No Token Provided" })
  }

  try {
    const decoded = jwt.verify(token, SECRET_KEY)
    req.user = decoded // Now TypeScript recognizes this property
    next() // Ensure the function moves to the next middleware
  } catch (error) {
    return res.status(401).json({ message: "Unauthorized - Invalid Token" })
  }
}
