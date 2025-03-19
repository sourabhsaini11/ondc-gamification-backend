import fs from "fs"
import csvParser from "csv-parser"
import { PrismaClient } from "@prisma/client"
import moment from "moment-timezone"
import { blake2b } from "blakejs"
import { logger } from "../shared/logger"
// import dayjs from "dayjs"

const prisma = new PrismaClient()

export const parseAndStoreCsv = async (
  filePath: string,
  userId: number,
): Promise<{ success: boolean; message: string }> => {
  const records: {
    uid: any
    name: any
    order_id: any
    order_status: any
    timestamp_created: Date
    timestamp_updated: Date
    domain: any
    buyer_app_id: any
    total_price: number
    // shipping_charges: number
    // taxes: number
    // discount: number
    // convenience_fee: number
    uploaded_by: number
  }[] = []
  // const uidFirstOrderTimestamp = new Map()
  const recordMap = new Map<string, { orderStatus: string; buyerAppId: string }>()
  let rowCount = 0
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on("data", (row) => {
        try {
          rowCount++
          if (rowCount > 100000) {
            return reject({ success: false, message: "Record length exceeded 100000" })
          }

          let check = false
          const emptyFields: string[] = []
          const normalizedRow = Object.fromEntries(
            Object.entries(row).map(([key, value]) => {
              const normalizedKey = key.trim().toLowerCase().replace(/\s+/g, "_")

              if (value == "" || value == undefined || value === null) {
                check = true
                if (!emptyFields.includes(normalizedKey)) {
                  emptyFields.push(normalizedKey)
                }
              }

              if (key == "order_status") {
                const normalizedValue = key.trim().toLowerCase().replace(/\s+/g, "_")
                value = normalizedValue
              }

              return [normalizedKey, value]
            }),
          )
          if (check) {
            console.error("Values can't be empty")
            return reject({
              success: false,
              message: `The following fields are empty or invalid: ${emptyFields.join(", ")} at index:${rowCount}`,
            })
          }

          const requiredFields = [
            "phone_number",
            "name",
            "order_id",
            "order_status",
            "timestamp_created",
            "domain",
            // "buyer_app_id",
            "total_price",
            // "shipping_charges",
            // "taxes",
            // "discount",
            // "conveniance_fee",
          ]

          const rowKeys = Object.keys(normalizedRow) // Get all keys in row
          const missingFields = requiredFields.filter((field) => !normalizedRow[field])

          if (missingFields.length > 0) {
            console.error(`❌ Missing Fields: ${missingFields.join(", ")}`)
            return reject({ success: false, message: `Fields are missing: ${missingFields.join(", ")}` })
          }

          requiredFields.push("timestamp_updated")

          const extraFields = rowKeys.filter((key) => !requiredFields.includes(key))
          if (extraFields.length > 0) {
            console.error(`❌ Unexpected Fields: ${extraFields.join(", ")}`)
            return reject({ success: false, message: `Unexpected fields found: ${extraFields.join(", ")}` })
          }

          const orderId = normalizedRow["order_id"]
          const existingRecord = recordMap.get(orderId as string)

          if (existingRecord) {
            if (
              existingRecord.orderStatus.toLowerCase() == "created" &&
              String(normalizedRow["order_status"]).toLowerCase() == "created" &&
              existingRecord.buyerAppId === String(userId)
            ) {
              return reject({
                success: false,
                message: `Duplicate Order ID: ${orderId} with status 'created' found multiple times for the same buyer at index:${rowCount}`,
              })
            }
          }

          //async IIFE to fetch Userid

          const timestampStr: any = normalizedRow["timestamp_created"] // Example: "2025-02-24 2:00:00"
          const timestampCreated: Date = moment
            .tz(timestampStr, "YYYY-MM-DD HH:mm:ss", "Asia/Kolkata")
            .add(5, "hours")
            .add(30, "minutes")
            .toDate()

          if (isNaN(timestampCreated.getTime())) {
            console.log(`Invalid timestamp for order ${orderId}`)
            return reject({ success: false, message: `Invalid timestamp for order ${orderId} at index:${rowCount}` })
          }

          // if (
          //   !recordMap.has(orderId as string) ||
          //   recordMap.get(orderId as string)?.orderStatus !== normalizedRow["order_status"] ||
          //   recordMap.get(orderId as string)?.buyerAppId !== normalizedRow["buyer_app_id"]
          // ) {
          records.push({
            uid: String(normalizedRow["phone_number"])?.trim(),
            name: normalizedRow["name"],
            order_id: orderId,
            order_status: String(normalizedRow["order_status"])?.toLowerCase(),
            timestamp_created: timestampCreated,
            timestamp_updated: new Date(String(normalizedRow["timestamp_updated"])) || timestampCreated, // timestamp_Updated update
            domain: normalizedRow["domain"],
            buyer_app_id: String(userId),
            total_price: parseFloat(String(normalizedRow["total_price"])) || 0,
            // shipping_charges: parseFloat(String(normalizedRow["shipping_charges"])) || 0,
            // taxes: parseFloat(String(normalizedRow["taxes"])) || 0,
            // discount: parseFloat(String(normalizedRow["discount"])) || 0,
            // convenience_fee: parseFloat(String(normalizedRow["conveniance_fee"])) || 0,
            uploaded_by: userId,
          })

          // Store order_id with its order_status in Map
          recordMap.set(orderId as string, {
            orderStatus: normalizedRow["order_status"] as string,
            buyerAppId: normalizedRow["buyer_app_id"] as string,
          })
          // } else {
          //   console.warn(`Duplicate order_id ${orderId} with status ${normalizedRow["order_status"]} skipped.`)
          //   return reject({ success: false, message: `Duplicate Order Id: ${orderId} Exists` })
          // }

          // records.filter((order)=>ordersExist.includes(order.))
          // console.log("records", records)
        } catch (error: any) {
          console.error("❌ Error processing row:", error)
          return reject({ success: false, message: error })
        }
      })
      .on("end", async () => {
        try {
          if (records.length === 0) {
            console.log("⚠️ No valid records found in the CSV file")
            return resolve({ success: false, message: "No valid records found in the CSV file" })
          }

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

          // await prisma.$executeRawUnsafe(`ALTER TABLE "orderData" DISABLE TRIGGER rewardledger_trigger;`)

          if (newOrders.length > 0) {
            const processedNewOrders = await processNewOrders(newOrders)
            await bulkInsertDataIntoDb(processedNewOrders)
          }

          if (cancellations.length > 0) {
            const processedCancelOrders = await processCancellations(cancellations)
            await bulkInsertDataIntoDb(processedCancelOrders)
          }

          // await prisma.$executeRawUnsafe(`ALTER TABLE "orderData" ENABLE TRIGGER rewardledger_trigger;`)

          // await updateHighestGmvAndOrdersForDay()

          // console.log("newOrders", newOrders)
          // console.log("cancellations", cancellations)

          console.log("✅ CSV data stored successfully")
          resolve({ success: true, message: "CSV data stored successfully" })
        } catch (error) {
          console.error("❌ Error storing CSV data:", error)
          reject({ success: false, message: "Error storing CSV data: " + error })
        } finally {
          fs.unlinkSync(filePath)
          await prisma.$disconnect()
        }
      })
      .on("error", (error) => {
        console.error("❌ Error reading CSV file:", error)
        reject({ success: false, message: "Error reading CSV file: " + error })
      })
  })
}

export const aggregateDailyGmvAndPoints = async () => {
  try {
    console.log("🔄 Aggregating daily GMV and points...")

    // Get distinct dates using prisma.$queryRawUnsafe
    //     const uniqueDates: any = (await prisma.$queryRawUnsafe<{ date: Date }[]>(
    //       `SELECT DISTINCT DATE(timestamp_created AT TIME ZONE 'Asia/Kolkata') AS date
    // FROM "orderData";
    // `,
    //     )) as { date: Date }[]

    const uniqueDates: any = (await prisma.$queryRawUnsafe(
      `SELECT DISTINCT DATE(timestamp_created AT TIME ZONE 'Asia/Kolkata') AS date 
  FROM "orderData";`,
    )) as { date: Date }[]

    console.log("uniqueDates", uniqueDates)

    for (const { date } of uniqueDates) {
      // Find the game with the highest GMV for the date
      const highestGmv: any = (await prisma.$queryRawUnsafe(
        `SELECT id, game_id 
         FROM "orderData" 
         WHERE DATE(timestamp_created AT TIME ZONE 'Asia/Kolkata') = '${date.toISOString().split("T")[0]}' 
         GROUP BY id, game_id 
         ORDER BY SUM(gmv) DESC 
         LIMIT 1;`,
      )) as { game_id: string }[]

      console.log("highestGmv", highestGmv)

      // Find the game with the highest order count for the date
      const highestOrders: any = (await prisma.$queryRawUnsafe(
        `SELECT id, game_id 
         FROM "orderData" 
         WHERE DATE(timestamp_created AT TIME ZONE 'Asia/Kolkata') = '${date.toISOString().split("T")[0]}' 
         GROUP BY id, game_id 
         ORDER BY COUNT(order_id) DESC 
         LIMIT 1;`,
      )) as { game_id: string }[]

      console.log("highestOrders", highestOrders)

      const topGmvGame: any = highestGmv[0]
      const topOrdersGame: any = highestOrders[0]

      if (topGmvGame && topOrdersGame) {
        if (topGmvGame.id === topOrdersGame.id) {
          await prisma.$queryRawUnsafe(
            `UPDATE "orderData" 
             SET points = points + 200
             WHERE id = $1;`,
            topGmvGame.id,
          )
          console.log(`✅ Updated id ${topGmvGame.id} with 200 points`)
        } else {
          await prisma.$queryRawUnsafe(
            `UPDATE "orderData" 
             SET points = points + 100
             WHERE id = $1;`,
            topGmvGame.id,
          )
          console.log(`✅ Updated id ${topGmvGame.id} with 100 points`)

          await prisma.$queryRawUnsafe(
            `UPDATE "orderData" 
             SET points = points + 100
             WHERE id = $1;`,
            topOrdersGame.id,
          )
          console.log(`✅ Updated id ${topOrdersGame.id} with 100 points`)
        }
      }
    }

    console.log("✅ Daily GMV and points aggregation completed.")
  } catch (error) {
    console.error("❌ Error aggregating daily GMV and points:", error)
  }
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
        // const timestampCreated: any = parseTimestamp(row.timestamp_created)
        const timestampCreated: any = row.timestamp_created
        console.log("tiemstampCreated", timestampCreated, new Date(timestampCreated), row.timestamp_created)
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

        console.log("existingUser-----", existingUser)

        let game_id,
          lastStreakDate,
          streakCount = 1
        let phone_number
        if (existingUser) {
          game_id = existingUser.game_id
          lastStreakDate = existingUser.last_streak_date || timestampCreated
          streakCount = existingUser.streak_count
        } else {
          if (!uidFirstOrderTimestamp[uid]) {
            // uidFirstOrderTimestamp[uid] = timestampCreated.format("HH")
            uidFirstOrderTimestamp[uid] = String(new Date(timestampCreated).getUTCHours()).padStart(2, "0")
          }

          // const firstName = row.name ? row.name.split(" ")[0] : "User"
          const fullUid = uid

          // Hash the lastUidDigits using BLAKE2b-512
          // const hash = blake2b(lastUidDigits, undefined, 64) // 64-byte (512-bit) hash
          // const hashedlastUidDigits = Buffer.from(hash).toString("hex")
          // console.log("hashedlastUidDigits", hashedlastUidDigits)
          lastStreakDate = timestampCreated
          // const temp_id = `${firstName}${uidFirstOrderTimestamp[uid]}${lastUidDigits}`
          // const temp_id = `${firstUidDigits}${firstName}${lastUidDigits}`
          const temp_id = `${fullUid}`
          const hash = blake2b(temp_id, undefined, 64) // 64-byte (512-bit) hash
          const hashedId = Buffer.from(hash).toString("hex")
          game_id = hashedId
          logger.info(`The GameID is: ${game_id}`)
          // phone_number = "XXXXXX" + lastUidDigits
        }

        // Calculate GMV
        const gmv = parseFloat(row.total_price) || 0

        rewardledgerUpdate(
          game_id,
          row.order_id,
          gmv,
          Math.floor(gmv / 10),
          "GMV & Points",
          true,
          row.timestamp_created,
        )

        console.log("first---", timestampCreated, timestampCreated.toISOString(), row.timestamp_created)

        // Handle streak logic
        const { streakMaintain, newStreakCount, newLastStreakDate }: any = processStreak(
          lastStreakDate,
          timestampCreated,
          streakCount,
        )

        console.log("newStreakCount", newStreakCount, streakCount)
        const points = await calculatePoints(
          game_id,
          gmv,
          uid,
          newStreakCount,
          "newOrder",
          timestampCreated,
          0,
          row.order_id,
          // row.order_status,
        )
        // await rewardledgerUpdate(game_id, row.order_id, points, "Points Assigned for the order")
        const orderCount = await getTodayOrderCount2(uid, timestampCreated, row.order_id)

        console.log("sec---", timestampCreated, timestampCreated.toISOString(), row.timestamp_created)

        // console.log("potins", game_id, points)

        processedData.push({
          ...row,
          phone_number,
          game_id,
          points: points,
          entry_updated: true,
          same_day_order_count: orderCount + 1,
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
  logger.info("Showing Cancellation orders!")
  console.log(cancellations)

  try {
    for (const row of cancellations) {
      try {
        const orderId = row.order_id
        const orderStatus = (row.order_status || "").toLowerCase()
        // const timestampCreated: any = parseTimestamp(row.timestamp_created)
        const timestampCreated: any = row.timestamp_created

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
        })
        // above is getting the number of order_id with not status of cancelled

        // underline is getting the number of uid with order status of cancelled
        const canceledOrderCount =
          originalOrder &&
          (await prisma.orderData.count({
            where: {
              uid: originalOrder.uid,
              order_status: {
                in: ["cancelled", "partially_cancelled"], // Only count orders that are canceled or partially canceled
              },
            },
          }))

        console.log("canceledOrderCount", canceledOrderCount)
        // Finding if the current user with the gameid and timestamp created has been winner of any type from the DailyWinnner
        const ifBeenAWinner = await prisma.dailyWinner.findMany({
          where: {
            AND: [{ game_id: originalOrder?.game_id }, { winning_date: originalOrder?.timestamp_created }],
          },
        })
        console.log("ifBeenAWinner", ifBeenAWinner)

        // now check how many points to be deducted

        // now i need to check if canceledOrderCount = 1 ? -150 points
        // if count = 2 ? point = 0
        // else blacklist the player
        // then deduct
        if (canceledOrderCount && canceledOrderCount >= 3) {
          console.log(`${originalOrder?.game_id} is blacklisted `)
        }

        const totalPoints =
          originalOrder &&
          (await prisma.orderData.groupBy({
            by: ["uid"], // Group by user ID
            _sum: {
              points: true, // Sum the points for each user
            },
            where: {
              uid: originalOrder.uid, // Filter for the specific user
              order_status: "created", // Only consider orders with status 'created'
            },
          }))
        // const tPoint = totalPoints ? -totalPoints : 0
        console.log(`Total points for user ${originalOrder?.uid}:`, totalPoints)

        if (!originalOrder) {
          console.log(`Original order not found for cancellation: ${orderId}`)
          return
        }

        const {
          points: originalPoints,
          game_id: gameId,
          uid,
          last_streak_date,
          gmv: originalGmv,
          order_status,
          same_day_order_count,
          streak_count,
        } = originalOrder

        console.log("originalGmv", originalGmv), order_status

        // Function to safely parse floats and handle negative values
        const safeFloat = (value: number | number, defaultValue: number = 0): number => {
          const num = parseFloat(value.toString())
          return isNaN(num) ? defaultValue : Math.abs(num)
        }

        // const basePrice = safeFloat(row.base_price, 0)
        // const shippingCharges = safeFloat(row.shipping_charges, 0)
        // const taxes = safeFloat(row.taxes, 0)
        // const convenienceFee = safeFloat(row.convenience_fee, 0)
        // const discount = safeFloat(row.discount, 0)

        // Calculate new GMV
        let newGmv = safeFloat(row.total_price, 0)

        // Calculate adjustment
        let pointsAdjustment
        if (orderStatus === "cancelled") {
          newGmv = originalGmv // Full cancellation resets GMV
          pointsAdjustment = -originalPoints
          rewardledgerUpdate(
            gameId,
            orderId,
            -newGmv,
            -originalPoints,
            "Cancelled GMV & Points",
            false,
            timestampCreated,
          )
          // if (canceledOrderCount == 0) {
          //   console.log("------->")
          //   pointsAdjustment = -(originalPoints + 200)
          // } else if (canceledOrderCount == 1) {
          //   pointsAdjustment = -tPoint
          // }

          await deductPointsForHigherSameDayOrders(uid, orderId, timestampCreated, gameId, same_day_order_count)
          await deductStreakPointsForFutureOrders(uid, orderId, timestampCreated, gameId, streak_count)

          // checking for the change in the leaderboard
          const Result: any = await prisma.dailyWinner.findFirst({
            where: {
              game_id: gameId,
            },
          })
          if (Result) {
            const topUsers: any = await prisma.rewardLedger.findMany({
              where: {
                created_at: Result[0].winningDate,
              },
              orderBy: {
                points: "desc", // Sort by points in descending order to get the top users
              },
              take: 2, // Only take the top 2 users
            })
            if (topUsers[0].points - pointsAdjustment < topUsers[1].points) {
              // await rewardledgerUpdate(
              //   gameId,
              //   orderId,
              //   Result[0].winning_date,
              //   -100,
              //   "Points deducted for removing from the poisition",
              // )
            }
          }
          // const Result: any = await prisma.$executeRawUnsafe(`
          //     Select game_id,points from dailyWinner limit 2
          //   `)
          // if (Result[0].game_id == gameId) {
          //   if (Result[0]?.points + pointsAdjustment < Result[1]?.points) {
          //     await rewardledgerUpdate(
          //       Result[0].game_id,
          //       orderId,
          //       timestampCreated,
          //       -100,
          //       "Points deducted for losing the poisition",
          //     )
          //   }
          // }
        } else {
          // Partially cancelled, recalculate points with streak as 0
          const newPoints = await calculatePoints(
            gameId,
            newGmv,
            uid,
            0,
            "partial",
            timestampCreated,
            originalGmv,
            orderId,
          )

          pointsAdjustment = newPoints - originalPoints // 110 - 210 = -110
          const gmvAdjustment = originalGmv - newGmv
          rewardledgerUpdate(
            gameId,
            orderId,
            gmvAdjustment,
            pointsAdjustment,
            "Adjusted GMV & Points",
            false,
            timestampCreated,
          )
        }

        // rewardledgerUpdate(
        //   gameId,
        //   row.order_id,
        //   -newGmv,
        //   -Math.floor(newGmv / 10),
        //   "Adjusted GMV & Points",
        //   true,
        //   timestampCreated,
        // )
        console.log("first---", timestampCreated, timestampCreated.toISOString())

        streakCancellation(
          originalOrder.last_streak_date,
          originalOrder.timestamp_created,
          originalOrder.streak_count,
          originalOrder.uid,
        )
        processedData.push({
          ...row,
          game_id: gameId,
          points: pointsAdjustment,
          entry_updated: true,
          streak_maintain: true,
          highest_gmv_for_day: false,
          highest_orders_for_day: false,
          gmv: newGmv,
          // total_price: row.total_price,
          // shipping_charges: shippingCharges,
          // taxes: taxes,
          // convenience_fee: convenienceFee,
          // discount: discount,
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

export const updateHighestGmvAndOrdersForDay = async () => {
  try {
    // Step 1: Reset highest_gmv_for_day and highest_orders_for_day for all records
    await prisma.orderData.updateMany({
      data: {
        highest_gmv_for_day: false,
        highest_orders_for_day: false,
      },
    })

    // Find the game_id with the highest GMV for each day
    // const highestGmvResults = await prisma.orderData.groupBy({
    //   by: ["game_id", "timestamp_created"],
    //   _sum: { points: true },
    //   orderBy: {
    //     _sum: { points: "desc" }, // Order by highest GMV
    //   },
    // })

    const highestGmvResults: any = await prisma.$queryRaw`
      WITH aggregated_orders AS (
        SELECT 
          game_id, 
          DATE(timestamp_created) AS order_date, 
          SUM(points) AS total_points
        FROM "orderData"
        GROUP BY game_id, order_date
      ),
      ranked_orders AS (
        SELECT 
          game_id, 
          order_date, 
          total_points,
          RANK() OVER (PARTITION BY order_date ORDER BY total_points DESC) AS rank
        FROM aggregated_orders
      )
      SELECT game_id, order_date, total_points
      FROM ranked_orders
      WHERE rank = 1 AND total_points > 0  -- Exclude zero-point records
      ORDER BY order_date DESC;
    `

    console.log("highestGmvResults", highestGmvResults)

    // Update highest_gmv_for_day for the top GMV game_id per day
    for (const { game_id, order_date } of highestGmvResults as any[]) {
      if (!order_date) {
        console.error("order_date is undefined for game_id:", game_id)
        continue // Skip invalid data
      }

      try {
        // Convert order_date to a valid Date object
        const istDate = new Date(order_date)

        if (isNaN(istDate.getTime())) {
          console.error("Invalid Date detected:", order_date)
          continue // Skip this iteration if date is invalid
        }

        // IST Offset in milliseconds
        const istOffset = 5.5 * 60 * 60 * 1000

        // Convert to IST start of day
        const startOfDayIST = new Date(istDate)
        startOfDayIST.setUTCHours(0, 0, 0, 0)

        // Convert to IST end of day
        const endOfDayIST = new Date(istDate)
        endOfDayIST.setUTCHours(23, 59, 59, 999)

        // Convert back to UTC for Prisma
        const startOfDayUTC = new Date(startOfDayIST.getTime() - istOffset)
        const endOfDayUTC = new Date(endOfDayIST.getTime() - istOffset)

        console.log(`Updating highest_gmv_for_day for game_id: ${game_id} from ${startOfDayUTC} to ${endOfDayUTC}`)

        await prisma.orderData.updateMany({
          where: {
            game_id,
            timestamp_created: {
              gte: startOfDayUTC, // Start of IST day in UTC
              lt: endOfDayUTC, // End of IST day in UTC
            },
          },
          data: {
            highest_gmv_for_day: true,
          },
        })
      } catch (error) {
        console.error("Error processing order_date:", order_date, error)
      }
    }

    // Step 4: Create a view for highest orders and GMV on the same day
    const highestOrdersResults = await prisma.$queryRaw`
      -- WITH aggregated_orders AS (
      --   SELECT game_id, DATE(timestamp_created) as order_date, COUNT(order_id) AS total_orders 
      --   FROM "orderData" 
      --   GROUP BY game_id, order_date
      -- )
      -- SELECT game_id, order_date 
      -- FROM aggregated_orders 
      -- ORDER BY total_orders DESC
      WITH filtered_orders AS (
      SELECT order_id
      FROM "orderData"
      GROUP BY order_id
      HAVING COUNT(*) = 1 OR COUNT(DISTINCT order_status) = 1
      ),
      aggregated_orders AS (
          SELECT game_id, DATE(timestamp_created) AS order_date, COUNT(order_id) AS total_orders
          FROM "orderData"
          WHERE order_id IN (SELECT order_id FROM filtered_orders)
          GROUP BY game_id, order_date
      )
      SELECT game_id, order_date, total_orders,order_id,timestamp_created
      FROM aggregated_orders
      ORDER BY total_orders DESC;
    `
    console.log("highestOrdersResults", highestOrdersResults)

    // Step 5: Update highest_orders_for_day for the top order count game_id per day
    for (const {
      game_id,
      order_id,
      order_date,
      total_orders,
      total_gmv,
      max_orders,
      max_gmv,
    } of highestOrdersResults as any[]) {
      // If this player has the highest GMV for the day as well, award extra points
      console.log("order_id", order_id)
      const isHighestGMVUser = highestGmvResults.some(
        (result: any) => result.game_id === game_id && result.timestamp_created === order_date,
      )

      // Deduct points from the old highest GMV and orders player if they were replaced
      if (isHighestGMVUser) {
        await prisma.leaderboard.update({
          where: { game_id },
          data: {
            total_points: {
              decrement: 100,
            },
          },
        })
        // await rewardledgerUpdate(
        //   game_id,
        //   order_id,
        //   0,
        //   -100,
        //   "For not maintaing the highest poistion in for highest gmv",
        //   false,
        // )
      }

      console.log("total_orders", total_orders, total_gmv, max_orders, max_gmv)

      // // Award 100 points for the highest GMV and 100 for the highest order count
      // if (total_orders === max_orders) {
      //   await prisma.leaderboard.update({
      //     where: { game_id },
      //     data: {
      //       total_points: {
      //         increment: 100,
      //       },
      //     },
      //   })
      // }

      // // Now award 100 points to the top GMV holder for this day
      // if (total_gmv === max_gmv) {
      //   await prisma.leaderboard.update({
      //     where: { game_id },
      //     data: {
      //       total_points: {
      //         increment: 100,
      //       },
      //     },
      //   })
      // }

      // Step 6: Add the player to the leaderboard if both GMV and orders are highest
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

const calculatePoints = async (
  game_id: string,
  gmv: number,
  uid: string,
  streakCount: number,
  condition: string,
  timestamp: any,
  originalGmv: number,
  orderId: string,
  // order_status?: string,
) => {
  gmv = Math.max(0, parseFloat(gmv.toString()))

  let points = 0
  const gmvPoints = Math.floor(gmv / 10)
  points += gmvPoints
  points += 10

  if (condition === "partial") {
    // await rewardledgerUpdate(game_id, orderId, 0, -10.0, "base Points deducted for part cancel ", true)
    // await rewardledgerUpdate(
    //   game_id,
    //   orderId,
    //   0,
    //   -Math.floor(gmv / 10),
    //   " Points deducted for part cancel ",
    //   true,
    //   timestamp,
    // )
    // ? Why are we sending points + 50 in the case of originalGMV excedding 1000 & current GMV deceding 1000
    if (originalGmv > 1000 && gmv < 1000) {
      await rewardledgerUpdate(game_id, orderId, 0, -50.0, "GMV < 1000 in partial cancellation ", true, timestamp)
      return points + 50
    }

    console.log("here in partial")
    return points
  } else {
    await rewardledgerUpdate(game_id, orderId, 0, +10.0, "base Points awarded for the order ", true, timestamp)
    // await rewardledgerUpdate(
    //   game_id,
    //   orderId,
    //   0,
    //   +Math.floor(gmv / 10),
    //   " Points awarded for the order ",
    //   true,
    //   timestamp,
    // )
  }

  if (gmv > 1000) {
    points += 50
    await rewardledgerUpdate(game_id, orderId, 0, +50.0, "GMV Greater 1000", true, timestamp)
  }

  try {
    console.log("==========>", timestamp)
    const orderCount = await getTodayOrderCount2(uid, timestamp, orderId)
    console.log("==========>", orderCount, timestamp)
    points += orderCount * 5
    // if (orderCount > 1) {
    await rewardledgerUpdate(
      game_id,
      orderId,
      0,
      +orderCount * 5,
      `Points for Repeated Order ${orderCount} in a day`,
      true,
      timestamp,
    )
    // }
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
    const eligibleBonus = Math.max(
      ...Object.keys(streakBonuses)
        .map(Number)
        .filter((key) => key <= streakCount),
    )

    console.log("eligibleBonus", eligibleBonus, streakBonuses[streakCount])

    if (streakBonuses[streakCount]) {
      points += streakBonuses[streakCount]
      await rewardledgerUpdate(
        game_id,
        orderId,
        0,
        +streakBonuses[streakCount],
        "Points assigned for Streak maintaince ",
        true,
        timestamp,
      )
    }
  }

  console.log("points", points)
  return points
}

const deductStreakPointsForFutureOrders = async (
  uid: string,
  orderId: string,
  timestamp_created: string,
  game_id: string,
  streak_count: number,
) => {
  try {
    const startOfDay = new Date(timestamp_created)
    startOfDay.setHours(0, 0, 0, 0)
    const endOfDay = new Date(timestamp_created)
    endOfDay.setHours(23, 59, 59, 999)

    console.log("streak_count", streak_count)

    // Streak bonus map
    const streakBonuses: Record<number, number> = {
      3: 20,
      7: 30,
      10: 100,
      14: 200,
      21: 500,
      28: 700,
    }

    const canceledOrders = await prisma.orderData.findMany({
      where: {
        uid: uid,
        game_id: game_id,
        timestamp_created: { gte: startOfDay, lte: endOfDay },
        order_status: "cancelled",
      },
      select: { order_id: true },
    })
    const canceledOrderIds = canceledOrders.map((order: any) => order.order_id)

    // Check if more orders exist for the same user, game, and day
    const sameDayOrders = await prisma.orderData.findMany({
      where: {
        uid: uid,
        game_id: game_id,
        timestamp_created: { gte: startOfDay, lte: endOfDay },
        NOT: { order_id: { in: [...canceledOrderIds, orderId] } },
      },
    })

    console.log("sameDayOrders", sameDayOrders)

    // Deduct points for the current order based on its streak count
    // let currentOrderPointsToDeduct = 0
    // currentOrderPointsToDeduct = currentOrderPointsToDeduct + streakBonuses[streak_count]

    // const currentOrder: any = await prisma.orderData.findFirst({
    //   where: { order_id: orderId, uid: uid, game_id: game_id },
    //   select: { id: true },
    // })

    // const updatedCurrentStreak = Math.max(streak_count - 1, 1)

    // await prisma.orderData.update({
    //   where: { id: currentOrder.id },
    //   data: {
    //     points: currentOrderPointsToDeduct > 0 ? { decrement: currentOrderPointsToDeduct } : undefined,
    //     streak_count: 0,
    //     updated_by_lambda: new Date().toISOString(),
    //   },
    // })
    // console.log(
    //   `Deducted ${currentOrderPointsToDeduct} points from current order ${orderId}, streak updated to ${updatedCurrentStreak}`,
    // )

    // If other orders exist on the same day, we skip deductions for future orders
    if (sameDayOrders.length > 0) {
      console.log(`Skipping future deductions because other orders exist for game_id ${game_id} on this day.`)
      return
    }

    console.log(
      `No other orders found for game_id ${game_id} on this day, proceeding with streak reduction for future orders.`,
    )

    // Find all future orders (same day and later) for the same user & game
    const futureOrders = await prisma.orderData.findMany({
      where: {
        uid: uid,
        game_id: game_id,
        timestamp_created: { gte: startOfDay },
        NOT: { order_id: orderId },
      },
      orderBy: { timestamp_created: "asc" },
    })

    console.log("futureOrders", futureOrders)

    let totalDeducted = 0

    for (const order of futureOrders) {
      console.log("Processing order:", order.order_id)

      if (order.streak_count === 1) {
        console.log(`Stopping deduction at order ${order.order_id} (streak == 1)`)
        break
      }

      let pointsToDeduct = 0
      pointsToDeduct = pointsToDeduct + streakBonuses[order.streak_count]

      const updatedStreak = Math.max(order.streak_count - streak_count, 1)
      console.log("updatedStreak", order.streak_count, updatedStreak)

      const logs = await prisma.orderData.update({
        where: { id: order.id },
        data: {
          points: pointsToDeduct > 0 ? { decrement: pointsToDeduct } : undefined,
          streak_count: updatedStreak,
          updated_by_lambda: new Date().toISOString(),
        },
      })
      console.log("logs", logs)

      if (pointsToDeduct > 0) {
        totalDeducted += pointsToDeduct
        console.log(`Deducted ${pointsToDeduct} points from order ${order.order_id}`)
        await rewardledgerUpdate(
          game_id,
          order.order_id,
          0,
          -pointsToDeduct,
          "Streak deduction",
          false,
          order.timestamp_created,
        )
      } else {
        console.log(`No points deducted for order ${order.order_id}, but streak count was reduced.`)
      }
    }

    console.log(`Total points deducted: ${totalDeducted}`)
  } catch (error) {
    console.error("Error deducting streak points:", error)
  }
}

const deductPointsForHigherSameDayOrders = async (
  uid: string,
  orderId: string,
  timestamp_created: string,
  game_id: string,
  same_day_order_count: number,
) => {
  try {
    const startOfDay = new Date(timestamp_created)
    startOfDay.setHours(0, 0, 0, 0)

    const endOfDay = new Date(timestamp_created)
    endOfDay.setHours(23, 59, 59, 999)

    const allOrders = await prisma.orderData.findMany({
      where: {
        uid: uid, // Same user
        game_id: game_id, // Same game
        timestamp_created: {
          gte: startOfDay,
          lt: endOfDay,
        },
        same_day_order_count: {
          gt: same_day_order_count,
        },
        NOT: {
          order_id: orderId,
        },
      },
    })

    console.log("updatedOrders2", allOrders)

    const updatedOrders = await prisma.orderData.updateMany({
      where: {
        uid: uid, // Same user
        game_id: game_id, // Same game
        timestamp_created: {
          gte: startOfDay,
          lt: endOfDay,
        },
        same_day_order_count: {
          gt: same_day_order_count,
        },
        NOT: {
          order_id: orderId,
        },
      },
      data: {
        points: {
          decrement: 5,
        },
        updated_by_lambda: new Date().toISOString(),
      },
    })

    console.log("updatedOrders", updatedOrders, allOrders)
    if (updatedOrders.count > 0) {
      await Promise.all(
        allOrders.map((order: any) =>
          rewardledgerUpdate(game_id, order.order_id, 0, -5, "Same-day order penalty", false, order.timestamp_created),
        ),
      )
    }

    console.log(`Updated ${updatedOrders.count} orders by deducting 5 points.`)
  } catch (error) {
    console.error("Error updating points:", error)
  }
}

const getTodayOrderCount2 = async (uid: string, timestamp: any, order_id: string) => {
  try {
    console.log("timestamp2", timestamp)
    const startOfDay = new Date(timestamp)
    startOfDay.setHours(0, 0, 0, 0)

    const endOfDay = new Date(timestamp)
    endOfDay.setHours(23, 59, 59, 999)

    // Get all order_id values that have at least one "cancelled" order
    const cancelledOrders = await prisma.orderData.findMany({
      where: {
        uid: uid,
        timestamp_created: {
          gte: startOfDay,
          lt: endOfDay,
        },
        order_status: "cancelled",
      },
      select: {
        order_id: true,
      },
    })

    const cancelledOrderIds = cancelledOrders.map((order: any) => order.order_id)

    // Add the provided order_id to the exclusion list
    if (order_id) {
      cancelledOrderIds.push(order_id)
    }

    // Count orders, excluding those with a "cancelled" order_id and the given order_id
    const totalOrdersToday = await prisma.orderData.count({
      where: {
        uid: uid,
        timestamp_created: {
          gte: startOfDay,
          lt: endOfDay,
        },
        NOT: {
          order_id: { in: cancelledOrderIds },
        },
      },
    })

    return totalOrdersToday
  } catch (error) {
    console.error(`Error fetching order count for ${uid}:`, error)
    return 0
  }
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

const streakCancellation = async (lastStreakDate: any, currentTimestamp: any, streakCount: any, uid: string) => {
  /**
   * Process streak logic and return updated values.
   */
  try {
    let streakMaintain = true
    let newStreakCount = streakCount || 1
    let newLastStreakDate = lastStreakDate || currentTimestamp
    const todayCount = await getTodayOrderCount(uid)

    if (lastStreakDate) {
      const lastStreakDay = moment(lastStreakDate).startOf("day")
      const currentDay = moment(currentTimestamp).startOf("day")
      const dayDifference = currentDay.diff(lastStreakDay, "days")

      if (dayDifference === 1) {
        newStreakCount += 1
        newLastStreakDate = currentTimestamp
      } else if (dayDifference > 1) {
        newStreakCount = 1 // Streak reset
        streakMaintain = false
        newLastStreakDate = currentTimestamp
      }
    }

    if (todayCount === 0) {
      newStreakCount = 0
      streakMaintain = false
      return { streakMaintain, newStreakCount, newLastStreakDate }
    }

    return { streakMaintain, newStreakCount, newLastStreakDate }
  } catch (err) {
    console.error("Error processing streak", err)
    return {
      streakMaintain: false,
      newStreakCount: 1,
      newLastStreakDate: currentTimestamp,
    }
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

    // const todayCount = await getTodayOrderCount(uid)
    // if (todayCount === 0) {
    //   newStreakCount = 0
    //   streakMaintain = false
    //   return { streakMaintain, newStreakCount, newLastStreakDate }
    // }

    console.log("lastStreakDate", lastStreakDate)

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
    return {
      streakMaintain: false,
      newStreakCount: 1,
      newLastStreakDate: currentTimestamp,
    }
  }
}

const bulkInsertDataIntoDb = async (data: any) => {
  /**
   * Bulk inserts data into the database using Prisma.
   */
  if (!data || data.length === 0) return
  console.log("row", JSON.stringify(data[0].uploaded_by))

  type OrderId = string | number
  type OrderStatus = string

  const orderRecords: { order_id: OrderId; order_status: OrderStatus }[] = await prisma.orderData.findMany({
    select: { order_id: true, order_status: true },
  })

  const existingOrdersMap = new Map<OrderId, Set<OrderStatus>>()

  orderRecords.forEach(({ order_id, order_status }) => {
    if (!existingOrdersMap.has(order_id)) {
      existingOrdersMap.set(order_id, new Set())
    }

    existingOrdersMap.get(order_id)?.add(order_status)
  })

  const usersWithExcessiveCancellations = await prisma.orderData.groupBy({
    by: ["game_id"], // Group by user ID & game_id
    _count: { order_status: true }, // Count number of orders per user per game
    where: {
      order_status: {
        in: ["cancelled"],
      },
    },
    having: {
      order_status: {
        _count: {
          gte: 3, // Users with 3 or more cancellations
        },
      },
    },
  })

  // Step 2: Create a Set of {uid, game_id} combinations to filter data
  const blacklistedUsers = new Set(usersWithExcessiveCancellations.map(({ game_id }: { game_id: string }) => game_id))

  // // Step 3: Filter out users from `data` who match the blacklist
  const filteredData = data.filter((item: any) => !blacklistedUsers.has(item.game_id))

  // // Filter new orders where either order_id is new OR order_status is new for an existing order_id
  const newOrders = filteredData.filter(
    (row: any) =>
      !existingOrdersMap.has(row.order_id) || // New order_id
      !existingOrdersMap.get(row.order_id)?.has(row.order_status), // New status for existing order_id
  )

  try {
    const formattedData = newOrders.map((row: any) => ({
      name: row.name,
      order_id: row.order_id,
      order_status: row.order_status,
      timestamp_created: row.timestamp_created,
      timestamp_updated: row.timestamp_updated,
      domain: row.domain,
      buyer_app_id: row.buyer_app_id,
      total_price: parseFloat(row.total_price || 0),
      // shipping_charges: parseFloat(row.shipping_charges || 0),
      // taxes: parseFloat(row.taxes || 0),
      // discount: parseFloat(row.discount || 0),
      // convenience_fee: parseFloat(row.convenience_fee || 0),
      uid: row.uid,
      game_id: row.game_id,
      points: parseFloat(row.points || 0),
      entry_updated: row.entry_updated,
      streak_maintain: row.streak_maintain,
      same_day_order_count: row.same_day_order_count || 1,
      highest_gmv_for_day: row.highest_gmv_for_day,
      highest_orders_for_day: row.highest_orders_for_day,
      updated_by_lambda: row.updated_by_lambda,
      gmv: row.gmv,
      streak_count: row.streak_count,
      uploaded_by: row.uploaded_by || Number(1),
      last_streak_date: row.last_streak_date,
    }))

    const insertedData = await prisma.orderData.createMany({ data: formattedData })
    console.log("The inserted Data is: ", insertedData)
    console.log(`Bulk data inserted successfully.`)
    // await prisma.$executeRawUnsafe(`SELECT update_leaderboard_manual()`)
  } catch (error) {
    console.error(`Error inserting bulk data`, error)
  }
}

export const parseTimestamp = (timestampStr: any) => {
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
      where: { uploaded_by: userId },
      orderBy: { timestamp_created: "desc" },
      skip,
      take: limit,
    })

    // Get total count for pagination metadata
    const totalOrders = await prisma.orderData.count({
      where: { uploaded_by: userId },
    })

    return { orders, totalOrders }
  } catch (error) {
    console.error("Error fetching user orders:", error)
    throw new Error("Failed to fetch user orders")
  }
}

export const getUserOrdersForCSV = async (userId: number) => {
  try {
    const orders = await prisma.orderData.findMany({
      where: { uploaded_by: userId },
      orderBy: { timestamp_created: "desc" },
    })

    return { orders }
  } catch (error) {
    console.error("Error fetching user orders:", error)
    throw new Error("Failed to fetch user orders")
  }
}

export const rewardLedgerTrigger = async () => {
  try {
    console.log("🔄 Setting up rewardledger trigger...")

    // Create or replace the function
    const res = await prisma.$executeRawUnsafe(`
  CREATE OR REPLACE FUNCTION rewardledger_function()
RETURNS TRIGGER AS $$
DECLARE
    order_count INT;
    streak_days INT;
    streak_bonus INT;
    order_points DECIMAL;
    streakBonuses JSONB;
    current_game_id TEXT;
    current_order_count INT;
    last_order_date DATE;
    is_cancelled BOOLEAN;
BEGIN
    -- Streak bonuses map for consecutive days
    streakBonuses := '{"3": 20, "7": 30, "10": 100, "14": 200, "21": 500, "28": 700}';

    -- Get the game_id and order status for the current order
    current_game_id := NEW.game_id;
    is_cancelled := NEW.order_status = 'cancelled';  -- Only fully cancelled orders deduct points

    -- Create a temporary table to store RepeatOrderCount data
    CREATE TEMP TABLE IF NOT EXISTS RepeatOrderCount (
        game_id TEXT,
        order_id TEXT,
        timestamp_created DATE,
        count INT
    ) ON COMMIT DROP;

    -- Check if game_id and timestamp_created exist in RepeatOrderCount
    SELECT count
    INTO current_order_count
    FROM RepeatOrderCount
    WHERE game_id = current_game_id
      AND timestamp_created = DATE(NEW.timestamp_created);

    IF FOUND THEN
        -- Increment the count for repeat orders on the same day
        UPDATE RepeatOrderCount
        SET count = count + 1
        WHERE game_id = current_game_id
          AND timestamp_created = DATE(NEW.timestamp_created);

        -- Get the new order count for this game_id and day
        SELECT count INTO order_count
        FROM RepeatOrderCount
        WHERE game_id = current_game_id
          AND timestamp_created = DATE(NEW.timestamp_created);

        -- Calculate order points (order_count * 5 points)
        order_points := order_count * 5;
    ELSE
        -- Create a new entry for this game_id and timestamp_created if not found
        INSERT INTO RepeatOrderCount (game_id, order_id, timestamp_created, count)
        VALUES (current_game_id, NEW.order_id, DATE(NEW.timestamp_created), 1);

        -- Assign points for the first order on this day
        order_points := 5;
    END IF;

    -- Insert into rewardledger for the current order
    IF is_cancelled THEN
        -- Deduct points for the cancelled order
        INSERT INTO rewardledger (order_id, game_id, points, reason, updated_at)
        VALUES (NEW.order_id, current_game_id, -order_points, 'Order cancelled - points deducted', NOW());
    ELSE
        -- Insert into rewardledger for the current order
        INSERT INTO rewardledger (order_id, game_id, points, reason, updated_at)
        VALUES (NEW.order_id, current_game_id, order_points, 'Order placed - points assigned based on order count', NOW());
    END IF;
END;
$$ LANGUAGE plpgsql;

            streak_bonus := (streakBonuses ->> '3')::INT;
        END IF;
    END IF;

    -- Ensure daily order points are always inserted
    INSERT INTO rewardledger (order_id, game_id, points, reason, updated_at)
    VALUES 
        (NEW.order_id, NEW.game_id, order_points, 'Daily order points', NOW()),
        (NEW.order_id, NEW.game_id, gmv_points, 'GMV-based bonus', NOW());

    -- Insert high-value bonus if applicable
    IF high_value_bonus > 0 THEN
        INSERT INTO rewardledger (order_id, game_id, points, reason, updated_at)
        VALUES (NEW.order_id, NEW.game_id, high_value_bonus, 'High GMV order bonus', NOW());
    END IF;

    -- Insert streak bonus if applicable
    IF streak_bonus > 0 THEN
        INSERT INTO rewardledger (order_id, game_id, points, reason, updated_at)
        VALUES (NEW.order_id, NEW.game_id, streak_bonus, 'Streak bonus - consecutive orders', NOW());
    END IF;

    RAISE NOTICE 'Rewards added for order_id %: Order Points %, GMV Points %, High-Value Bonus %, Streak Bonus %', 
        NEW.order_id, order_points, gmv_points, high_value_bonus, streak_bonus;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
    `)

    console.log("✅ RewardLedger trigger function created successfully!", res)

    // Remove old rewardledger trigger if it exists
    await prisma.$executeRawUnsafe(`
      DROP TRIGGER IF EXISTS trigger_reward_ledger ON "orderData";
    `)
    console.log("🔄 Old rewardledger trigger removed (if it existed).")

    // Create the new rewardledger trigger
    await prisma.$executeRawUnsafe(`
      CREATE TRIGGER rewardledger_trigger
      AFTER INSERT OR UPDATE OR DELETE ON "orderData"
      FOR EACH ROW
      EXECUTE FUNCTION rewardledger_function();
    `)
    console.log("✅ New rewardledger trigger created successfully.")
  } catch (error) {
    console.error("❌ Error setting up rewardledger trigger:", error)
  }
}

const rewardledgerUpdate = async (
  game_id: string,
  order_id: string,
  gmv: number,
  points: number,
  reason: string,
  positive: boolean,
  created_at: any,
) => {
  try {
    console.log("pointssssssssssss", points, -points)
    if (!(points == 0 && gmv == 0)) {
      await prisma.rewardLedger.create({
        data: {
          order_id: order_id,
          game_id: game_id,
          created_at,

          gmv: gmv,
          points: positive ? points : points,
          reason: reason,
        },
      })
    }
  } catch (error) {
    console.log("error", error)
  }
}
