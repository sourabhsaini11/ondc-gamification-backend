import fs from "fs"
import csvParser from "csv-parser"
import { PrismaClient } from "@prisma/client"

const prisma = new PrismaClient()

interface CsvRow {
  uid: string
  name: string
  orderId: string
  orderStatus: string
  timestampCreated: string
  timestampUpdated: string
  category: string
  buyerAppId: string
  basePrice: string
  shippingCharges: string
  taxes: string
  discount: string
  convenienceFee: string
  sellerId: string
}

export const parseAndStoreCsv = async (filePath: string): Promise<void> => {
  const records: CsvRow[] = []

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on("data", (row) => {
        const formattedRow: CsvRow = {
          uid: row["uid"],
          name: row["Name"],
          orderId: row["Order ID"],
          orderStatus: row["Order Status"],
          timestampCreated: row["Timestamp Created"],
          timestampUpdated: row["Timestamp Updated"],
          category: row["Category"],
          buyerAppId: row["Buyer App ID"],
          basePrice: row["Base Price"],
          shippingCharges: row["Shipping Charges"],
          taxes: row["Taxes"],
          discount: row["Discount"],
          convenienceFee: row["Conveniance fee"],
          sellerId: row["Seller ID"],
        }
        records.push(formattedRow)
      })
      .on("end", async () => {
        try {
          if (records.length > 0) {
            await prisma.orderData.createMany({
              data: records,
              skipDuplicates: true,
            })
            console.log("✅ CSV data stored successfully")

            await updateLeaderboard()
          } else {
            console.log("⚠️ No valid records found in the CSV file")
          }

          resolve()
        } catch (error) {
          console.error("❌ Error storing CSV data:", error)
          reject(error)
        } finally {
          fs.unlinkSync(filePath)
          await prisma.$disconnect()
        }
      })
      .on("error", (error) => {
        console.error("❌ Error reading CSV file:", error)
        reject(error)
      })
  })
}

const updateLeaderboard = async () => {
  try {
    // await prisma.$executeRaw`TRUNCATE TABLE "Leaderboard";`

    const users = await prisma.orderData.findMany()
    const leaderboardData = users.map((user) => {
      const basePoints = 10
      const gmv =
        parseFloat(user.basePrice) +
        parseFloat(user.shippingCharges) +
        parseFloat(user.taxes) -
        parseFloat(user.discount) +
        parseFloat(user.convenienceFee)
      const gmvPoints = Math.floor(gmv / 10)
      const highValueBonus = gmv > 1000 ? 50 : 0
      const totalPoints = basePoints + gmvPoints + highValueBonus

      return {
        uid: user.uid,
        name: user.name,
        basePoints,
        gmvPoints,
        highValueBonus,
        totalPoints,
        updatedAt: new Date(),
      }
    })

    await prisma.leaderboard.createMany({ data: leaderboardData, skipDuplicates: true })
    console.log("🏆 Leaderboard updated successfully:", leaderboardData)
  } catch (error) {
    console.error("❌ Error updating leaderboard:", error)
    throw error
  }
}

console.log("updateLeaderboard", updateLeaderboard)

export const getOrders = async (page: number = 1, pageSize: number = 10) => {
  try {
    const skip = (page - 1) * pageSize
    const orders = await prisma.orderData.findMany({
      skip,
      take: pageSize,
    })

    const totalOrders = await prisma.orderData.count()
    const totalPages = Math.ceil(totalOrders / pageSize)

    console.log("✅ Retrieved orders:", orders)
    return {
      orders,
      currentPage: page,
      totalPages,
      totalOrders,
    }
  } catch (error) {
    console.error("❌ Error retrieving orders:", error)
    throw error
  } finally {
    await prisma.$disconnect()
  }
}
