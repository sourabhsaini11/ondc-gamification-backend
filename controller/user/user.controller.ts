import { Request, Response } from "express";
import { prisma } from "../../prisma/index";



const userController = {
    async login (req: Request, res: Response) {
        const {email, name, password} = req.body;
        try {
            const user = await prisma.user.findUnique({
                where: {
                    email: email,
                }
            })
            if(user){
                return res.status(400).json({message: "User Already exist with this email"})
            } else {
                const user = await prisma.user.create({
                    data: {
                        email: email,
                        name: name,
                        password: password  
                    }})
                    return res.status(200).json({message: "User createdd successfully", id: user.id})
            }
        } catch (error: any) {
            return res.status(500).json({error: error.message})
        }
        
        
}
}
export default userController;