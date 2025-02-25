import fs from "fs"
import csvParser from "csv-parser"
import { PrismaClient } from "@prisma/client"
import moment from "moment-timezone"
// import dayjs from "dayjs"

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
//   base_price: string
//   shipping_charges: string
//   taxes: string
//   discount: string
//   convenience_fee: string
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
//           base_price: row["Base Price"],
//           shipping_charges: row["Shipping Charges"],
//           taxes: row["Taxes"],
//           discount: row["Discount"],
//           convenience_fee: row["Conveniance fee"],
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

export const parseAndStoreCsv = async (filePath: string, userId: number): Promise<void> => {
  const records: {
    uid: any
    name: any
    order_id: any
    order_status: any
    timestamp_created: Date
    timestamp_updated: Date
    category: any
    buyer_app_id: any
    base_price: number
    shipping_charges: number
    taxes: number
    discount: number
    convenience_fee: number
    seller_id: any
    uploaded_by: number
  }[] = []
  // const uidFirstOrderTimestamp = new Map()

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on("data", (row) => {
        try {
          const normalizedRow = Object.fromEntries(
            Object.entries(row).map(([key, value]) => {
              const normalizedKey = key.trim().toLowerCase().replace(/\s+/g, "_")
              return [normalizedKey, value]
            }),
          )

          const orderId = normalizedRow["order_id"]
          const timestampStr: any = normalizedRow["timestamp_created"] // Example: "2025-02-24 2:00:00"
          const timestampCreated: Date = moment
            .tz(timestampStr, "YYYY-MM-DD HH:mm:ss", "Asia/Kolkata")
            .add(5, "hours")
            .add(30, "minutes")
            .toDate()

          if (isNaN(timestampCreated.getTime())) {
            console.log(`Invalid timestamp for order ${orderId}`)
            return
          }

          records.push({
            uid: String(normalizedRow["phone_number"])?.trim(),
            name: normalizedRow["name"],
            order_id: orderId,
            order_status: String(normalizedRow["order_status"])?.toLowerCase(),
            timestamp_created: timestampCreated,
            timestamp_updated: new Date(String(normalizedRow["timestamp_updated"])),
            category: normalizedRow["category"],
            buyer_app_id: normalizedRow["buyer_app_id"],
            base_price: parseFloat(String(normalizedRow["base_price"])) || 0,
            shipping_charges: parseFloat(String(normalizedRow["shipping_charges"])) || 0,
            taxes: parseFloat(String(normalizedRow["taxes"])) || 0,
            discount: parseFloat(String(normalizedRow["discount"])) || 0,
            convenience_fee: parseFloat(String(normalizedRow["conveniance_fee"])) || 0,
            seller_id: normalizedRow["seller_id"],
            uploaded_by: userId,
          })
          // records.filter((order)=>ordersExist.includes(order.))
          console.log("records", records)
        } catch (err) {
          console.error("❌ Error processing row:", err)
        }
      })
      .on("end", async () => {
        try {
          if (records.length === 0) {
            console.log("⚠️ No valid records found in the CSV file")
            return resolve()
          }

          // console.log("records", records)

          const newOrders: any = []
          const cancellations: any = []

          records.forEach((row) => {
            const orderStatus = (row.order_status || "").toLowerCase()
            if (["cancelled", "partially_cancelled"].includes(orderStatus)) {
              cancellations.push(row)
            } else {
              newOrders.push(row)
            }
          })

          if (newOrders.length > 0) {
            const processedNewOrders = await processNewOrders(newOrders)
            await bulkInsertDataIntoDb(processedNewOrders)
          }

          if (cancellations.length > 0) {
            const processedCancelOrders = await processCancellations(cancellations)
            await bulkInsertDataIntoDb(processedCancelOrders)
          }

          await updateHighestGmvAndOrdersForDay()

          // console.log("newOrders", newOrders)
          // console.log("cancellations", cancellations)

          console.log("✅ CSV data stored successfully")
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

// const updateLeaderboard = async () => {
//   try {
//     // await prisma.$executeRaw`TRUNCATE TABLE "Leaderboard";`

//     const users = await prisma.orderData.findMany()
//     const myMap = new Map()

//     const leaderboardData = users.map((user: any) => {
//       let userOrderData = myMap.get(user.uid)

//       const currentDate = new Date(user.timestamp_created)
//       const currentDay = currentDate.toISOString().split("T")[0] // Format as YYYY-MM-DD

//       // If the user has made an order before today, check and add repeat order points
//       if (userOrderData && userOrderData.lastOrderDate === currentDay) {
//         // Increment order count for today
//         userOrderData.orderCount += 1
//       } else {
//         // If it's the first order of the day, reset the order count and update the last order date
//         userOrderData = {
//           orderCount: 1,
//           lastOrderDate: currentDay,
//           totalPoints: 0,
//         }
//       }

//       // Update the Map with the latest data for the user
//       myMap.set(user.uid, userOrderData)

//       const basePoints = 10
//       const gmv =
//         parseFloat(user.base_price) +
//         parseFloat(user.shipping_charges) +
//         parseFloat(user.taxes) -
//         parseFloat(user.discount) +
//         parseFloat(user.convenience_fee)
//       const gmvPoints = Math.floor(gmv / 10)
//       const highValueBonus = gmv > 1000 ? 50 : 0
//       const { streakcount, streakMaintain, currentTimestamp } = ProcessSteak(
//         new Date(user.timestamp_created),
//         new Date(user.timestamp_created),
//         Number(user.streak_count),
//       )
//       let repeatOrderPoints = 0
//       if (userOrderData.orderCount > 1) {
//         // Increment points for each repeat order (2nd, 3rd, etc.)
//         repeatOrderPoints = 15 + (userOrderData.orderCount - 2) * 5 // 15 points for the 2nd order, 20 for 3rd, etc.
//       }

//       const Points = CalculatePoints(gmv, Number(user.streak_count), user.uid)
//       const totalPoints = basePoints + gmvPoints + highValueBonus + Number(Points) + repeatOrderPoints
//       return {
//         uid: user.uid,
//         name: user.name,
//         basePoints,
//         gmvPoints,
//         highValueBonus,
//         totalPoints,
//         streak: streakcount,
//         streak_maintain: streakMaintain,
//         last_streak_date: currentTimestamp,
//         updatedAt: new Date(),
//       }
//     })

//     // await prisma.leaderboard.createMany({ data: leaderboardData, skipDuplicates: true })
//     console.log("🏆 Leaderboard updated successfully:", leaderboardData)
//   } catch (error) {
//     console.error("❌ Error updating leaderboard:", error)
//     throw error
//   }
// }

// console.log("updateLeaderboard", updateLeaderboard)

export const getOrders = async (page: number = 1, pageSize: number = 100) => {
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

// const ProcessSteak = (lastStreakDate: Date, currentTimestamp: Date, streakcount: number) => {
//   let streakMaintain = true

//   if (lastStreakDate) {
//     const dayDifference = Math.floor((currentTimestamp.getTime() - lastStreakDate.getTime()) / (1000 * 3600 * 24))

//     if (dayDifference === 1) {
//       streakcount += 1 // Increment streak count for consecutive days
//     } else if (dayDifference > 1) {
//       streakcount = 1 // Reset streak count if the difference is more than 1 day
//       streakMaintain = false
//     } else if (dayDifference < 1) {
//       streakcount = streakcount || 1 // Ensure streak count is at least 1 if no difference
//     }
//   }

//   return { streakMaintain, streakcount, currentTimestamp }
// }

// const CalculatePoints = async (gmv: number, streakCount: number, uid: string) => {
//   gmv = Math.max(0, parseFloat(gmv.toString()))

//   let points = 10
//   points += Math.floor(gmv / 10)

//   if (gmv > 1000) {
//     points += 50
//   }

//   try {
//     // const orderCount = await getTodayOrderCount(uid)
//     const orderCount = 1
//     points += orderCount * 5
//   } catch (error) {
//     console.error(`Error calculating order count points for ${uid}:`, error)
//   }

//   if (streakCount > 0) {
//     const streakBonuses: { [key: number]: number } = {
//       3: 20,
//       7: 30,
//       10: 100,
//       14: 200,
//       21: 500,
//       28: 700,
//     }

//     if (streakBonuses[streakCount]) {
//       points += streakBonuses[streakCount]
//     }
//   }

//   return points
// }

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

const processNewOrders = async (orders: any) => {
  const processedData = []

  try {
    const uidFirstOrderTimestamp: any = {}

    for (const row of orders) {
      try {
        const uid = String(row.uid || "").trim()
        const timestampCreated: any = parseTimestamp(row.timestamp_created)
        console.log("tiemstampCreated", timestampCreated)
        // if (!timestampCreated.isValid()) {
        //   console.log(`Invalid timestamp for order ${row.order_id}`)
        //   continue
        // }

        // Get existing user data
        const existingUser = await prisma.orderData.findFirst({
          where: { uid: uid },
          orderBy: { timestamp_created: "desc" },
          select: { game_id: true, last_streak_date: true, streak_count: true },
        })

        // console.log("existingUser", existingUser)

        let game_id,
          lastStreakDate,
          streakCount = 1

        if (existingUser) {
          game_id = existingUser.game_id
          lastStreakDate = existingUser.last_streak_date || timestampCreated
          streakCount = existingUser.streak_count
        } else {
          if (!uidFirstOrderTimestamp[uid]) {
            uidFirstOrderTimestamp[uid] = timestampCreated.hour()
          }

          const firstName = row.name ? row.name.split(" ")[0] : "User"
          const lastUidDigits = uid.length >= 4 ? uid.slice(-4) : uid
          lastStreakDate = timestampCreated
          game_id = `${firstName}${uidFirstOrderTimestamp[uid]}${lastUidDigits}`
        }

        // Calculate GMV
        const gmv =
          (parseFloat(row.base_price) || 0) +
          (parseFloat(row.shipping_charges) || 0) +
          (parseFloat(row.taxes) || 0) +
          (parseFloat(row.convenience_fee) || 0)

        const points = await calculatePoints(gmv, uid, streakCount)
        // console.log("potins", game_id, points)

        // Handle streak logic
        // console.log("streakMaintain", lastStreakDate, timestampCreated, streakCount)
        const { streakMaintain, newStreakCount, newLastStreakDate }: any = processStreak(
          lastStreakDate,
          timestampCreated,
          streakCount,
        )

        processedData.push({
          ...row,
          game_id,
          points: points,
          entry_updated: true,
          streak_maintain: streakMaintain,
          highest_gmv_for_day: false,
          highest_orders_for_day: false,
          streak_count: newStreakCount,
          last_streak_date: newLastStreakDate,
          gmv: Math.floor(gmv),
          updated_by_lambda: new Date().toISOString(),
          timestamp_created: timestampCreated.toISOString(),
          timestamp_updated: new Date().toISOString(),
          uid: uid,
        })
      } catch (err: any) {
        console.error(`Error processing new order: ${err.message}`)
        continue
      }
    }

    return processedData
  } catch (err: any) {
    console.error(`Error processing new orders: ${err}`)
    return []
  }
}

const processCancellations = async (cancellations: any) => {
  const processedData = []

  try {
    for (const row of cancellations) {
      try {
        const orderId = row.order_id
        const orderStatus = (row.order_status || "").toLowerCase()
        const timestampCreated: any = parseTimestamp(row.timestamp_created)

        const originalOrder = await prisma.orderData.findFirst({
          where: {
            order_id: orderId,
            order_status: {
              notIn: ["cancelled", "partially_cancelled"],
            },
          },
          orderBy: {
            timestamp_created: "desc",
          },
          select: {
            points: true,
            gmv: true,
            game_id: true,
            uid: true,
            last_streak_date: true,
          },
        })

        if (!originalOrder) {
          console.log(`Original order not found for cancellation: ${orderId}`)
          continue
        }

        const { points: originalPoints, game_id: gameId, uid, last_streak_date } = originalOrder

        // Function to safely parse floats and handle negative values
        const safeFloat = (value: number | number, defaultValue: number = 0): number => {
          const num = parseFloat(value.toString())
          return isNaN(num) ? defaultValue : Math.abs(num)
        }

        const basePrice = safeFloat(row.base_price, 0)
        const shippingCharges = safeFloat(row.shipping_charges, 0)
        const taxes = safeFloat(row.taxes, 0)
        const convenienceFee = safeFloat(row.convenience_fee, 0)
        const discount = safeFloat(row.discount, 0)

        // Calculate new GMV
        let newGmv = basePrice + shippingCharges + taxes + convenienceFee - discount

        // Calculate adjustment
        let pointsAdjustment
        if (orderStatus === "cancelled") {
          newGmv = 0 // Full cancellation resets GMV
          pointsAdjustment = -originalPoints
        } else {
          // Partially cancelled, recalculate points with streak as 0
          const newPoints = await calculatePoints(newGmv, uid, 0)
          pointsAdjustment = newPoints - originalPoints
        }

        processedData.push({
          ...row,
          game_id: gameId,
          points: pointsAdjustment,
          entry_updated: true,
          streak_maintain: true,
          highest_gmv_for_day: false,
          highest_orders_for_day: false,
          gmv: newGmv,
          base_price: basePrice,
          shipping_charges: shippingCharges,
          taxes: taxes,
          convenience_fee: convenienceFee,
          discount: discount,
          updated_by_lambda: new Date().toISOString(),
          timestamp_created: timestampCreated.toISOString(),
          timestamp_updated: new Date().toISOString(),
          uid: uid,
          order_status: orderStatus,
          last_streak_date,
        })
      } catch (err) {
        console.error(`Error processing cancellation for order ${row.order_id}: ${err}`)
        continue
      }
    }

    return processedData
  } catch (err) {
    console.error(`Error processing cancellations: ${err}`)
    return []
  }
}

const updateHighestGmvAndOrdersForDay = async () => {
  try {
    // Reset highest_gmv_for_day and highest_orders_for_day for all records
    await prisma.orderData.updateMany({
      data: {
        highest_gmv_for_day: false,
        highest_orders_for_day: false,
      },
    })

    // Find the game_id with the highest GMV for each day
    const highestGmvResults = await prisma.orderData.groupBy({
      by: ["game_id", "timestamp_created"],
      _sum: { points: true },
      orderBy: {
        _sum: { points: "desc" }, // Order by highest GMV
      },
    })

    // Update highest_gmv_for_day for the top GMV game_id per day
    for (const { game_id, timestamp_created } of highestGmvResults as any[]) {
      await prisma.orderData.updateMany({
        where: {
          game_id,
          timestamp_created: {
            gte: new Date(timestamp_created),
            lt: new Date(new Date(timestamp_created).setDate(new Date(timestamp_created).getDate() + 1)),
          },
        },
        data: { highest_gmv_for_day: true },
      })
    }

    // Find the game_id with the highest number of orders for each day
    const highestOrdersResults = await prisma.$queryRaw`
      WITH aggregated_orders AS (
        SELECT game_id, DATE(timestamp_created) as order_date, COUNT(order_id) AS total_orders 
        FROM "orderData" 
        GROUP BY game_id, order_date
      )
      SELECT game_id, order_date 
      FROM aggregated_orders 
      ORDER BY total_orders DESC
    `
    console.log("highestOrdersResults", highestOrdersResults)

    // Update highest_orders_for_day for the top order count game_id per day
    for (const { game_id, order_date } of highestOrdersResults as any[]) {
      await prisma.orderData.updateMany({
        where: {
          game_id,
          timestamp_created: {
            gte: new Date(order_date),
            lt: new Date(new Date(order_date).setDate(new Date(order_date).getDate() + 1)),
          },
        },
        data: { highest_orders_for_day: true },
      })
    }

    console.log("Highest GMV and orders for the day updated successfully.")
  } catch (error) {
    console.error("Error updating highest GMV and orders for the day:", error)
  }
}

const calculatePoints = async (gmv: number, uid: string, streakCount: number) => {
  gmv = Math.max(0, parseFloat(gmv.toString()))

  let points = 10

  points += Math.floor(gmv / 10)

  if (gmv > 1000) {
    points += 50
  }

  try {
    const orderCount = await getTodayOrderCount(uid)
    console.log("==========>", orderCount)
    points += orderCount * 5
  } catch (error) {
    console.error(`Error calculating order count points for ${uid}:`, error)
  }

  if (streakCount > 0) {
    const streakBonuses: any = {
      3: 20,
      7: 30,
      10: 100,
      14: 200,
      21: 500,
      28: 700,
    }

    if (streakBonuses[streakCount]) {
      points += streakBonuses[streakCount]
    }
  }

  console.log("points", points)
  return points
}

const getTodayOrderCount = async (uid: string) => {
  try {
    const totalOrdersToday = await prisma.orderData.count({
      where: {
        uid: uid,
        timestamp_created: {
          gte: new Date(new Date().setHours(0, 0, 0, 0)),
          lt: new Date(new Date().setHours(23, 59, 59, 999)),
        },
      },
    })

    return totalOrdersToday
  } catch (error) {
    console.error(`Error fetching order count for ${uid}:`, error)
    return 0
  }
}

const processStreak = (lastStreakDate: any, currentTimestamp: any, streakCount: any) => {
  /**
   * Process streak logic and return updated values.
   */
  try {
    let streakMaintain = true
    let newStreakCount = streakCount || 1
    let newLastStreakDate = lastStreakDate || currentTimestamp

    if (lastStreakDate) {
      const lastStreakDay = moment(lastStreakDate).startOf("day")
      const currentDay = moment(currentTimestamp).startOf("day")
      const dayDifference = currentDay.diff(lastStreakDay, "days")

      if (dayDifference === 1) {
        newStreakCount += 1 // Continue streak
        newLastStreakDate = currentTimestamp
      } else if (dayDifference > 1) {
        newStreakCount = 1 // Streak reset
        streakMaintain = false
        newLastStreakDate = currentTimestamp
      }
    }

    return { streakMaintain, newStreakCount, newLastStreakDate }
  } catch (err) {
    console.error("Error processing streak", err)
    return { streakMaintain: false, newStreakCount: 1, newLastStreakDate: currentTimestamp }
  }
}

const bulkInsertDataIntoDb = async (data: any) => {
  /**
   * Bulk inserts data into the database using Prisma.
   */
  if (!data || data.length === 0) return
  console.log("row", JSON.stringify(data[0].uploaded_by))

  const orderRecords = await prisma.orderData.findMany({
    select: { order_id: true, order_status: true },
  });
  
  // Create a Map where the key is order_id and the value is a Set of order_status
  const existingOrdersMap = new Map<string, Set<string>>();
  
  orderRecords.forEach(({ order_id, order_status }) => {
    if (!existingOrdersMap.has(order_id)) {
      existingOrdersMap.set(order_id, new Set());
    }

    existingOrdersMap.get(order_id)?.add(order_status);
  });
  
  // Filter new orders where either order_id is new OR order_status is new for an existing order_id
  const newOrders = data.filter(
    (row: any) =>
      !existingOrdersMap.has(row.order_id) || // New order_id
      !existingOrdersMap.get(row.order_id)?.has(row.order_status) // New status for existing order_id
  );

  try {
    const formattedData = newOrders.map((row: any) => ({
      name: row.name,
      order_id: row.order_id,
      order_status: row.order_status,
      timestamp_created: row.timestamp_created,
      timestamp_updated: row.timestamp_updated,
      category: row.category,
      buyer_app_id: row.buyer_app_id,
      base_price: parseFloat(row.base_price || 0),
      shipping_charges: parseFloat(row.shipping_charges || 0),
      taxes: parseFloat(row.taxes || 0),
      discount: parseFloat(row.discount || 0),
      convenience_fee: parseFloat(row.convenience_fee || 0),
      seller_id: row.seller_id,
      uid: row.uid,
      game_id: row.game_id,
      points: parseFloat(row.points || 0),
      entry_updated: row.entry_updated,
      streak_maintain: row.streak_maintain,
      highest_gmv_for_day: row.highest_gmv_for_day,
      highest_orders_for_day: row.highest_orders_for_day,
      updated_by_lambda: row.updated_by_lambda,
      gmv: row.gmv,
      streak_count: row.streak_count,
      uploaded_by: row.uploaded_by || Number(1),
      last_streak_date: row.last_streak_date,
    }))

    await prisma.orderData.createMany({ data: formattedData })
    console.log(`Bulk data inserted successfully.`)
  } catch (error) {
    console.error(`Error inserting bulk data`, error)
  }
}

const parseTimestamp = (timestampStr: any) => {
  try {
    const parsedDate = moment(timestampStr, [moment.ISO_8601, "DD/MM/YYYY HH:mm:ss", "DD-MM-YYYY HH:mm:ss"], true)

    if (!parsedDate.isValid()) {
      console.error("parseTimestamp Error: Invalid timestamp format", timestampStr)
      return null
    }

    return parsedDate
  } catch (error) {
    console.error("Invalid timestamp format:", error)
    return null
  }
}

export const getUserOrders = async (userId: number, page: number = 1, limit: number = 10) => {
  try {
    console.log("userId", userId, "Page:", page, "Limit:", limit)

    const skip = (page - 1) * limit

    const orders = await prisma.orderData.findMany({
      where: { uploaded_by: 1 },
      orderBy: { timestamp_created: "desc" },
      skip,
      take: limit,
    })

    // Get total count for pagination metadata
    const totalOrders = await prisma.orderData.count({
      where: { uploaded_by: 1 },
    })

    return { orders, totalOrders }
  } catch (error) {
    console.error("Error fetching user orders:", error)
    throw new Error("Failed to fetch user orders")
  }
}
