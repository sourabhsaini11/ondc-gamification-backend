import fs from "fs"
import csvParser from "csv-parser"
import { PrismaClient } from "@prisma/client"

const prisma = new PrismaClient()

// interface CsvRow {
//   uid: string
//   name: string
//   orderId: string
//   orderStatus: string
//   timestampCreated: string
//   timestampUpdated: string
//   category: string
//   buyerAppId: string
//   basePrice: string
//   shippingCharges: string
//   taxes: string
//   discount: string
//   convenienceFee: string
//   sellerId: string
// }

// export const parseAndStoreCsv = async (filePath: string): Promise<void> => {
//   const records: CsvRow[] = []

//   return new Promise((resolve, reject) => {
//     fs.createReadStream(filePath)
//       .pipe(csvParser())
//       .on("data", (row) => {
//         const formattedRow: CsvRow = {
//           uid: row["uid"],
//           name: row["Name"],
//           orderId: row["Order ID"],
//           orderStatus: row["Order Status"],
//           timestampCreated: row["Timestamp Created"],
//           timestampUpdated: row["Timestamp Updated"],
//           category: row["Category"],
//           buyerAppId: row["Buyer App ID"],
//           basePrice: row["Base Price"],
//           shippingCharges: row["Shipping Charges"],
//           taxes: row["Taxes"],
//           discount: row["Discount"],
//           convenienceFee: row["Conveniance fee"],
//           sellerId: row["Seller ID"],
//         }
//         records.push(formattedRow)
//       })
//       .on("end", async () => {
//         try {
//           if (records.length > 0) {

//             for (let user of records){
//               const uid = user.uid;
//             const orderId = user.orderId;
//             const orderStatus = user.orderStatus;
//              const timestampCreated = user.timestampCreated





//             }





//             await prisma.orderData.createMany({
//               data: records,
//               skipDuplicates: true,
//             })
//             console.log("✅ CSV data stored successfully")

//             await updateLeaderboard()
//           } else {
//             console.log("⚠️ No valid records found in the CSV file")
//           }

//           resolve()
//         } catch (error) {
//           console.error("❌ Error storing CSV data:", error)
//           reject(error)
//         } finally {
//           fs.unlinkSync(filePath)
//           await prisma.$disconnect()
//         }
//       })
//       .on("error", (error) => {
//         console.error("❌ Error reading CSV file:", error)
//         reject(error)
//       })
//   })
// }


export const parseAndStoreCsv = async (filePath: string): Promise<void> => {
  const records: {
    uid: any
    name: any
    orderId: any
    orderStatus: any
    timestampCreated: Date
    timestampUpdated: Date
    category: any
    buyerAppId: any
    basePrice: number
    shippingCharges: number
    taxes: number
    discount: number
    convenienceFee: number
    sellerId: any
  }[] = []
  const uidFirstOrderTimestamp = new Map()

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on("data", (row) => {
        try {
          const uid = row["uid"]?.trim();
          const orderId = row["Order ID"];
          const orderStatus = row["Order Status"]?.toLowerCase();
          const timestampCreated = new Date(row["Timestamp Created"]);

          if (isNaN(timestampCreated.getTime())) {
            console.log(`Invalid timestamp for order ${orderId}`);
            return
          }

          records.push({
            uid,
            name: row["Name"],
            orderId,
            orderStatus,
            timestampCreated,
            timestampUpdated: new Date(row["Timestamp Updated"]),
            category: row["Category"],
            buyerAppId: row["Buyer App ID"],
            basePrice: parseFloat(row["Base Price"]) || 0,
            shippingCharges: parseFloat(row["Shipping Charges"]) || 0,
            taxes: parseFloat(row["Taxes"]) || 0,
            discount: parseFloat(row["Discount"]) || 0,
            convenienceFee: parseFloat(row["Conveniance fee"]) || 0,
            sellerId: row["Seller ID"],
          });
        } catch (err) {
          console.error("❌ Error processing row:", err);
        }
      })
      .on("end", async () => {
        try {
          if (records.length === 0) {
            console.log("⚠️ No valid records found in the CSV file");
            return resolve()
          }

          for (let user of records) {
            const uid = user.uid
            const orderId = user.orderId
            const orderStatus = user.orderStatus
            const timestampCreated = user.timestampCreated

            // Fetch existing order details
            const existingOrder = await prisma.orderData.findFirst({
              where: { orderId: orderId },
              orderBy: { timestampCreated: "desc" },
              select: {
                points: true,
                gmv: true,
                gameId: true,
                timestampCreated: true,
              },
            });

            // Handle cancellations and partial refunds
            if (existingOrder?.length > 0 && ["cancelled", "partially_cancelled"].includes(orderStatus)) {
              const { points: originalPoints } = existingOrder[0]
              let newGmv = 0
              let pointsAdjustment = -originalPoints

              if (orderStatus === "partially_cancelled") {
                newGmv = user.basePrice + user.shippingCharges + user.taxes + user.convenienceFee
                // pointsAdjustment = CalculatePoints(gmv, streakcount, uid) - originalPoints
                pointsAdjustment=0
              }

              const gameId = `aaa`

              await prisma.orderData.create({
                data: {
                  gameId: gameId,
                  name:"hh",
                  points: String(pointsAdjustment),
                  streakMaintain: "true",
                  highestGmvForDay: "false",
                  highestOrdersForDay: "false",
                  gmv: String(newGmv),
                  // updatedByLambda: new Date(),
                  timestampCreated,
                  timestampUpdated: new Date(),
                },
              })

              continue
            }

            // Regular Order Processing
            const existingUser: any = await prisma.$queryRaw`
              SELECT game_id, last_streak_date, streak_count FROM (
                SELECT game_id, last_streak_date, streak_count FROM td_data WHERE uid = ${uid}
                UNION
                SELECT game_id, last_streak_date, streak_count FROM mp_data WHERE uid = ${uid}
              ) combined_data LIMIT 1;
            `

            let gameId,
              // lastStreakDate,
              streakCount = 1
            if (existingUser.length > 0) {
              ({ game_id: gameId, streak_count: streakCount } = existingUser[0])
            } else {
              if (!uidFirstOrderTimestamp.has(uid)) {
                uidFirstOrderTimestamp.set(uid, timestampCreated.getHours())
              }

              const firstName = user.name?.split(" ")[0] || "User"
              const lastUidDigits = uid.slice(-4)
              gameId = `${firstName}${uidFirstOrderTimestamp.get(uid)}${lastUidDigits}`
              // lastStreakDate = timestampCreated
            }

            const gmv = user.basePrice + user.shippingCharges + user.taxes + user.convenienceFee

            // const points = CalculatePoints(gmv, 0, uid)
            // const {  currentTimestamp } = ProcessSteak(lastStreakDate, timestampCreated, streakCount)

            // Repeat Order Logic
            // let repeatOrderPoints = 0
            const userOrderData = uidFirstOrderTimestamp.get(uid)
            const currentDay = timestampCreated.toISOString().split("T")[0]

            if (userOrderData?.lastOrderDate === currentDay) {
              userOrderData.orderCount += 1
              // repeatOrderPoints = 15 + (userOrderData.orderCount - 2) * 5
            } else {
              uidFirstOrderTimestamp.set(uid, { lastOrderDate: currentDay, orderCount: 1 })
            }

            const totalPoints = 12
            await prisma.orderData.create({
              data: {
                gameId,
                uid,
                name:"hh",
                points: String(totalPoints),
                streakMaintain:"true",
                highestGmvForDay: "false",
                highestOrdersForDay: "false",
                streakcount: String(streakCount),
                // lastStreakDate: currentTimestamp,
                gmv: String(gmv),
                // updatedByLambda: new Date(),
                timestampCreated,
                timestampUpdated: new Date(),
              },
            });
          }

          console.log("✅ CSV data stored successfully");
          resolve();
        } catch (error) {
          console.error("❌ Error storing CSV data:", error);
          reject(error);
        } finally {
          fs.unlinkSync(filePath);
          await prisma.$disconnect();
        }
      })
      .on("error", (error) => {
        console.error("❌ Error reading CSV file:", error);
        reject(error);
      });
  });
};

const updateLeaderboard = async () => {
  try {
    // await prisma.$executeRaw`TRUNCATE TABLE "Leaderboard";`

    const users = await prisma.orderData.findMany()
    const myMap = new Map()
   
    const leaderboardData = users.map((user) => {
      let userOrderData = myMap.get(user.uid)

      const currentDate = new Date(user.timestampCreated)
      const currentDay = currentDate.toISOString().split("T")[0] // Format as YYYY-MM-DD
  
  // If the user has made an order before today, check and add repeat order points
  if (userOrderData && userOrderData.lastOrderDate === currentDay) {
    // Increment order count for today
        userOrderData.orderCount += 1
  } else {
    // If it's the first order of the day, reset the order count and update the last order date
        userOrderData = {
          orderCount: 1,
          lastOrderDate: currentDay,
          totalPoints: 0,
    };
  }

  // Update the Map with the latest data for the user
      myMap.set(user.uid, userOrderData)

      const basePoints = 10
      const gmv =
        parseFloat(user.basePrice) +
        parseFloat(user.shippingCharges) +
        parseFloat(user.taxes) -
        parseFloat(user.discount) +
        parseFloat(user.convenienceFee)
      const gmvPoints = Math.floor(gmv / 10)
      const highValueBonus = gmv > 1000 ? 50 : 0
      const { streakcount, streakMaintain, currentTimestamp } = ProcessSteak(
        new Date(user.timestampCreated),
        new Date(user.timestampCreated),
        Number(user.streakcount),
      )
      let repeatOrderPoints = 0;
      if (userOrderData.orderCount > 1) {
        // Increment points for each repeat order (2nd, 3rd, etc.)
        repeatOrderPoints = 15 + (userOrderData.orderCount - 2) * 5; // 15 points for the 2nd order, 20 for 3rd, etc.
      }

      const Points = CalculatePoints(gmv, Number(user.streakcount), user.uid)
      const totalPoints = basePoints + gmvPoints + highValueBonus + Number(Points) + repeatOrderPoints
      return {
        uid: user.uid,
        name: user.name,
        basePoints,
        gmvPoints,
        highValueBonus,
        totalPoints,
        streak: streakcount,
        streak_maintain: streakMaintain,
        last_streak_date: currentTimestamp,
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

const ProcessSteak = (lastStreakDate: Date, currentTimestamp: Date, streakcount: number) => {
  let streakMaintain = true

    if (lastStreakDate) {
    const dayDifference = Math.floor((currentTimestamp.getTime() - lastStreakDate.getTime()) / (1000 * 3600 * 24))
  
      if (dayDifference === 1) {
      streakcount += 1 // Increment streak count for consecutive days
      } else if (dayDifference > 1) {
      streakcount = 1 // Reset streak count if the difference is more than 1 day
      streakMaintain = false
      } else if (dayDifference < 1) {
      streakcount = streakcount || 1 // Ensure streak count is at least 1 if no difference
      }
    }
  
  return { streakMaintain, streakcount, currentTimestamp }
}

const CalculatePoints = async (gmv: number, streakCount: number, uid: string) => {
  gmv = Math.max(0, parseFloat(gmv.toString()))

  let points = 10
  points += Math.floor(gmv / 10)

  if (gmv > 1000) {
    points += 50
  }

  try {
    // const orderCount = await getTodayOrderCount(uid)
    const orderCount=1
    points += orderCount * 5
  } catch (error) {
    console.error(`Error calculating order count points for ${uid}:`, error)
  }

  if (streakCount > 0) {
    const streakBonuses: { [key: number]: number } = {
      3: 20,
      7: 30,
      10: 100,
      14: 200,
      21: 500,
      28: 700,
    };

    if (streakBonuses[streakCount]) {
      points += streakBonuses[streakCount]
    }
  }

  return points
}

// const getTodayOrderCount = async (uid: string): Promise<number> => {
//   try {
//     const ordersToday = await prisma.orderData.count({
//       where: {
//         uid: uid,
//         timestampCreated: {
//           gte: new Date(new Date().setHours(0, 0, 0, 0)),
//           lt: new Date(new Date().setHours(23, 59, 59, 999)),
//         },
//       },
//     });

//     return ordersToday
//   } catch (error) {
//     console.error("Error fetching today's order count:", error);
//     throw error
//   }
// }

