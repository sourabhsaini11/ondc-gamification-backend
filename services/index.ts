import fs from "fs"
import csvParser from "csv-parser"
import { PrismaClient } from "@prisma/client"
import moment from "moment-timezone"
import { blake2b } from "blakejs"
import { logger } from "../shared/logger"

const prisma = new PrismaClient()
export const findInvalidOrderStatus = (orders: any[]): { success: boolean; message?: string } => {
  const validStatuses = new Set(["active", "created", "partially_cancelled", "cancelled"])

  for (const order of orders) {
    if (!validStatuses.has(order.order_status)) {
      return { success: false, message: `Invalid order_status = ${order.order_status} for order_id: ${order.order_id}` }
    }
  }

  return { success: true }
}

export const findDuplicateOrderIdAndStatys = (orders: { order_id: string; order_status: string }[]) => {
  const seen = new Set<string>()
  for (const order of orders) {
    const key = `${order.order_id}-${order.order_status}`
    if (seen.has(key)) {
      return {
        success: false,
        message: `Duplicate found: Order ID ${order.order_id} with status ${order.order_status}`,
      }
    }

    seen.add(key)
  }

  return { success: true, message: "No duplicates found" }
}

const validatePhoneNumber = (phone_number: any, index: number): { success: boolean; message?: string } => {
  const phoneRegex = /^\d{3}XXX\d{4}$/ // Expected format: 733XXX1892

  if (!/^\d{10}$/.test(phone_number.replace(/X/g, "0"))) {
    return { success: false, message: `Invalid phone number at row: ${index}` }
  }

  if (!phoneRegex.test(phone_number)) {
    return { success: false, message: `Masking of phone number not followed for row: ${index}` }
  }

  if (/[^0-9X]/.test(phone_number)) {
    return { success: false, message: `Invalid phone number format at row: ${index}}` }
  }

  return { success: true }
}

const validateTotalPrice = (total_price: any, index: number): { success: boolean; message?: string } => {
  logger.info("total_price", total_price)
  if (typeof total_price !== "number" || isNaN(total_price) || /[^0-9.]/.test(total_price.toString())) {
    return { success: false, message: `Invalid total price at row ${index}` }
  }

  if (total_price <= 0) {
    return { success: false, message: `Issue with total price at row ${index}` }
  }

  return { success: true }
}
const validateOrderTimestamp = (orders: any[]): { success: boolean; message?: string } => {
  const now = new Date()
  const oneDayLater = new Date()
  oneDayLater.setDate(now.getDate() + 1) // Allow timestamps up to 1 day in the future

  for (const order of orders) {
    const { order_id, timestamp_created: timestamp } = order

    // Try to parse the timestamp
    const orderDate = new Date(timestamp)

    // Check if the parsed date is invalid
    if (isNaN(orderDate.getTime())) {
      return { success: false, message: `timestamp error at order_id ${order_id}` }
    }

    // Ensure it's not more than 1 day in the future
    if (orderDate > oneDayLater) {
      return { success: false, message: `future timestamp error at order_id ${order_id}` }
    }
  }

  return { success: true }
}

export const parseAndStoreCsv = async (
  filePath: string,
  userId: number,
): Promise<{ success: boolean; message: string }> => {
  const records: {
    uid: any
    order_id: any
    order_status: any
    timestamp_created: Date
    timestamp_updated: Date
    buyer_app_id?: any
    total_price: number
    uploaded_by: number
  }[] = []
  const recordMap = new Map<string, { orderStatus: string; buyerAppId: string }>()
  const partialMap = new Map<string, { orderStatus: string; buyerAppId: string }>()

  let rowCount = 0

  return new Promise((resolve, reject) => {
    const shouldAbort = false
    const stream = fs.createReadStream(filePath).pipe(csvParser())
    stream
      .on("data", (row) => {
        try {
          rowCount++
          if (rowCount > 100000) {
            return reject({ success: false, message: "Record length exceeded 100000" })
          }

          let check = false
          const emptyFields: string[] = []

          // filter csv rows
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

          const requiredFields = ["phone_number", "order_id", "order_status", "timestamp_created", "total_price"]

          const rowKeys = Object.keys(normalizedRow) // Get all keys in row
          const missingFields = requiredFields.filter((field) => !normalizedRow[field])

          if (missingFields.length > 0) {
            console.error(`❌ Missing Fields: ${missingFields.join(", ")}`)
            return reject({
              success: false,
              message: `mismatch for column name at column ${missingFields.join(", ")}`,
            })
          }

          requiredFields.push("timestamp_updated")

          const extraFields = rowKeys.filter((key) => !requiredFields.includes(key))
          if (extraFields.length > 0) {
            console.error(`❌ Unexpected Fields: ${extraFields.join(", ")}`)
            return reject({ success: false, message: `Unexpected fields found: ${extraFields.join(", ")}` })
          }

          const orderId: any = normalizedRow["order_id"]
          const existingRecord = recordMap.get(orderId as string)
          const orderStatus = String(normalizedRow["order_status"])?.toLowerCase()
          const validOrderStatus = ["active", "partially_cancelled", "cancelled"]

          if (!validOrderStatus.includes(orderStatus))
            return reject({
              success: false,
              message: `issue with order status at index:${rowCount}`,
            })

          if (existingRecord) {
            if (
              ((existingRecord.orderStatus.toLowerCase() == "created" &&
                String(normalizedRow["order_status"]).toLowerCase() == "created") ||
                (existingRecord.orderStatus.toLowerCase() == "active" &&
                  String(normalizedRow["order_status"]).toLowerCase() == "active")) &&
              existingRecord.buyerAppId === String(userId)
            ) {
              return reject({
                success: false,
                message: `Duplicate Order ID: ${orderId} with status ${existingRecord.orderStatus} found multiple times for the same buyer at index:${rowCount}`,
              })
            }
          }

          if (String(normalizedRow["order_status"]).toLowerCase() == "partially_cancelled") {
            const existingRecord = partialMap.get(orderId as string)
            if (existingRecord) {
              if (
                existingRecord.orderStatus.toLowerCase() == "partially_cancelled" &&
                String(normalizedRow["order_status"]).toLowerCase() == "partially_cancelled" &&
                existingRecord.buyerAppId === String(userId)
              ) {
                return reject({
                  success: false,
                  message: `Duplicate Order ID: ${orderId} with status 'partially_cancelled' found multiple times for the same buyer at index:${rowCount}`,
                })
              }
            }
          }

          //async IIFE to fetch Userid

          const timestampStr: any = normalizedRow["timestamp_created"] // Example: "2025-02-24 2:00:00"
          const totalPrice: number = parseFloat(String(normalizedRow["total_price"]))
          const timestampCreated: Date = moment
            .tz(timestampStr, "YYYY-MM-DD HH:mm:ss", "Asia/Kolkata")
            .add(5, "hours")
            .add(30, "minutes")
            .toDate()

          if (isNaN(timestampCreated.getTime())) {
            logger.info(`Invalid timestamp for order ${orderId}`)
            return reject({ success: false, message: `Invalid timestamp for order ${orderId} at index:${rowCount}` })
          }

          // check for invalid total_price
          const isInvalidTotalPrice = validateTotalPrice(totalPrice, rowCount)
          if (!isInvalidTotalPrice.success) {
            return reject({ success: false, message: isInvalidTotalPrice.message })
          }

          // check for invalid phone_number
          const isInvalidPhoneNumber = validatePhoneNumber(normalizedRow["phone_number"], rowCount)
          if (!isInvalidPhoneNumber.success) {
            return reject({ success: false, message: isInvalidPhoneNumber.message })
          }

          records.push({
            uid: String(normalizedRow["phone_number"])?.trim(),
            order_id: orderId,
            order_status: orderStatus,
            timestamp_created: timestampCreated,
            timestamp_updated: new Date(String(normalizedRow["timestamp_updated"])) || timestampCreated, // timestamp_Updated update
            buyer_app_id: String(userId),
            total_price: totalPrice,
            uploaded_by: userId,
          })

          logger.info("records12", records)

          // Store order_id with its order_status in Map
          recordMap.set(orderId as string, {
            orderStatus: normalizedRow["order_status"] as string,
            buyerAppId: normalizedRow["buyer_app_id"] as string,
          })
          partialMap.set(orderId as string, {
            orderStatus: normalizedRow["order_status"] as string,
            buyerAppId: normalizedRow["buyer_app_id"] as string,
          })
        } catch (error: any) {
          logger.info("error", error)
          stream.destroy()
          return reject({ success: false, message: error.message })
        }
      })
      .on("end", async () => {
        try {
          if (shouldAbort) return

          if (records.length === 0) {
            logger.info("recordsss", records)
            logger.info("⚠️ No valid records found in the CSV file")
            return resolve({ success: false, message: "No valid records found in the CSV file" })
          }

          for (const row of records) {
            if (await isDuplicateOrder(row.order_id, row.order_status, row.buyer_app_id)) {
              return reject({
                success: false,
                message: `Duplicate order ${row.order_id} (${row.order_status}) at row ${rowCount}`,
              })
            }
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
          // checks for invalid timestamp
          const isInvalidTimestamp = validateOrderTimestamp([...newOrders, ...cancellations])
          if (!isInvalidTimestamp.success) {
            return reject({ success: false, message: isInvalidTimestamp.message })
          }

          // check for invalid order_status
          const isInvalidOrderStatus: any = findInvalidOrderStatus([...newOrders, ...cancellations])
          if (!isInvalidOrderStatus.success) {
            return reject({ success: false, message: isInvalidOrderStatus.message })
          }

          // check for duplicate order id for same status
          const isDuplicateNewOrder = findDuplicateOrderIdAndStatys(newOrders)
          const isDuplicateCancellation = findDuplicateOrderIdAndStatys(cancellations)
          if (!isDuplicateNewOrder.success) {
            return reject({ success: false, message: isDuplicateNewOrder.message })
          }

          if (!isDuplicateCancellation.success) {
            return reject({ success: false, message: isDuplicateCancellation.message })
          }

          if (newOrders.length > 0) {
            const processedNewOrders = await processNewOrders(newOrders)
            await bulkInsertDataIntoDb(processedNewOrders)
          }

          if (cancellations.length > 0) {
            try {
              const processedCancelOrders = await processCancellations(cancellations)
              logger.info("processedCancelOrders", processedCancelOrders)
              await bulkInsertDataIntoDb(processedCancelOrders)
            } catch (error: any) {
              logger.info("Got Error in Cancellation", error)
              throw new Error(error.message)
            }
          }

          logger.info("✅ CSV data stored successfully")
          resolve({ success: true, message: "CSV data stored successfully" })
        } catch (error: any) {
          console.error("❌ Error storing CSV data:", error)
          reject({ success: false, message: "Error storing CSV data: " + error.message })
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
    logger.info("🔄 Aggregating daily GMV and points...")


    const uniqueDates: any = (await prisma.$queryRawUnsafe(
      `SELECT DISTINCT DATE(timestamp_created AT TIME ZONE 'Asia/Kolkata') AS date 
  FROM "orderData";`,
    )) as { date: Date }[]

    logger.info("uniqueDates", uniqueDates)

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

      logger.info("highestGmv", highestGmv)

      // Find the game with the highest order count for the date
      const highestOrders: any = (await prisma.$queryRawUnsafe(
        `SELECT id, game_id 
         FROM "orderData" 
         WHERE DATE(timestamp_created AT TIME ZONE 'Asia/Kolkata') = '${date.toISOString().split("T")[0]}' 
         GROUP BY id, game_id 
         ORDER BY COUNT(order_id) DESC 
         LIMIT 1;`,
      )) as { game_id: string }[]

      logger.info("highestOrders", highestOrders)

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
          logger.info(`✅ Updated id ${topGmvGame.id} with 200 points`)
        } else {
          await prisma.$queryRawUnsafe(
            `UPDATE "orderData" 
             SET points = points + 100
             WHERE id = $1;`,
            topGmvGame.id,
          )
          logger.info(`✅ Updated id ${topGmvGame.id} with 100 points`)

          await prisma.$queryRawUnsafe(
            `UPDATE "orderData" 
             SET points = points + 100
             WHERE id = $1;`,
            topOrdersGame.id,
          )
          logger.info(`✅ Updated id ${topOrdersGame.id} with 100 points`)
        }
      }
    }

    logger.info("✅ Daily GMV and points aggregation completed.")
  } catch (error) {
    console.error("❌ Error aggregating daily GMV and points:", error)
  }
}

export const search = async (game_id: string, format: string) => {
  try {
    logger.info("Format:", format, "Game ID:", game_id)

    const startDate = new Date()

    if (format === "daily") {
      startDate.setUTCHours(0, 0, 0, 0) // Start of the day UTC
    } else if (format === "weekly") {
      startDate.setUTCDate(startDate.getUTCDate() - 6)
      startDate.setUTCHours(0, 0, 0, 0)
    } else if (format === "monthly") {
      startDate.setUTCDate(startDate.getUTCDate() - 30)
      startDate.setUTCHours(0, 0, 0, 0)
    } else {
      throw new Error("Invalid format. Allowed values: 'daily', 'weekly', 'monthly'.")
    }

    // ✅ Ensure correct format for Prisma DateTime filter
    logger.info("Start Date Filter (UTC):", startDate.toISOString())

    const totalPoints = await prisma.$queryRaw`
  SELECT COALESCE(SUM(points), 0) AS total_points, game_id
  FROM rewardledger
  WHERE game_id LIKE ${game_id} || '%'
  AND created_at >= ${new Date(startDate).toISOString()}::timestamp AT TIME ZONE 'UTC'
  GROUP BY game_id
`

    logger.info("Total Points Result:", totalPoints)
    return totalPoints
  } catch (error) {
    console.error("Error in search function:", error)
    throw error
  }
}

export const search2 = async (game_id: string, format: string) => {
  try {
    console.log("Format:", format, "Game ID:", game_id)

    const startDate = new Date()

    if (format === "daily") {
      startDate.setUTCHours(0, 0, 0, 0) // Start of the day UTC
    } else if (format === "weekly") {
      startDate.setUTCDate(startDate.getUTCDate() - 6)
      startDate.setUTCHours(0, 0, 0, 0)
    } else if (format === "monthly") {
      startDate.setUTCDate(startDate.getUTCDate() - 30)
      startDate.setUTCHours(0, 0, 0, 0)
    } else {
      throw new Error("Invalid format. Allowed values: 'daily', 'weekly', 'monthly'.")
    }

    // ✅ Ensure correct format for Prisma DateTime filter
    console.log("Start Date Filter (UTC):", startDate.toISOString())

    const totalPoints = await prisma.$queryRaw`
  SELECT COALESCE(SUM(points), 0) AS total_points, game_id
  FROM rewardledgertesting
  WHERE game_id LIKE ${game_id} || '%'
  AND created_at >= ${new Date(startDate).toISOString()}::timestamp AT TIME ZONE 'UTC'
  GROUP BY game_id
`

    console.log("Total Points Result:", totalPoints)
    return totalPoints
  } catch (error) {
    console.error("Error in search function:", error)
    throw error
  }
}

export const getOrders = async (page: number = 1, pageSize: number = 100) => {
  try {
    const skip = (page - 1) * pageSize
    const orders = await prisma.orderData.findMany({
      skip,
      take: pageSize,
    })

    const totalOrders = await prisma.orderData.count()
    const totalPages = Math.ceil(totalOrders / pageSize)

    logger.info("✅ Retrieved orders:", orders)
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

const processNewOrders = async (orders: any) => {
  const processedData = []

  try {
    const uidFirstOrderTimestamp: any = {}

    for (const row of orders) {
      try {
        const uid = String(row.uid || "").trim()
        const timestampCreated: any = row.timestamp_created
        logger.info("tiemstampCreated", timestampCreated, new Date(timestampCreated), row.timestamp_created)

        // Get existing user data
        const existingUser = await prisma.orderData.findFirst({
          where: { uid: uid },
          orderBy: { timestamp_created: "desc" },
          select: { game_id: true, last_streak_date: true, streak_count: true },
        })

        logger.info("existingUser-----", existingUser)

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

          const fullUid = uid

          lastStreakDate = timestampCreated
          //GAME ID FORMATION
          const temp_id = `${fullUid}`
          const hash = blake2b(temp_id, undefined, 64) // 64-byte (512-bit) hash
          const hashedId = Buffer.from(hash).toString("hex")
          game_id = hashedId
          logger.info(`The GameID is: ${game_id}`)
        }

        logger.info(lastStreakDate)
        // Calculate GMV
        const gmv = parseFloat(row.total_price) || 0

        logger.info("first---", timestampCreated, timestampCreated.toISOString(), row.timestamp_created)

        logger.info("newStreakCount", streakCount)
        const points = await calculatePoints(
          game_id,
          gmv,
          uid,
          streakCount,
          "newOrder",
          timestampCreated,
          0,
          row.order_id,
        )
        const orderCount = await getTodayOrderCount2(uid, timestampCreated, row.order_id)

        logger.info("sec---", timestampCreated, timestampCreated.toISOString(), row.timestamp_created)


        processedData.push({
          ...row,
          phone_number,
          game_id,
          points: points,
          entry_updated: true,
          same_day_order_count: orderCount + 1,
          streak_maintain: true,
          highest_gmv_for_day: false,
          highest_orders_for_day: false,
          streak_count: 0,
          last_streak_date: new Date().toISOString(),
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
  logger.info(cancellations)

  try {
    for (const row of cancellations) {
      try {
        const orderId = row.order_id
        const orderStatus = (row.order_status || "").toLowerCase()
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
        logger.info("originalOrder in cancellation", originalOrder)

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

        logger.info("canceledOrderCount", canceledOrderCount)
        // Finding if the current user with the gameid and timestamp created has been winner of any type from the DailyWinnner
        const ifBeenAWinner = await prisma.dailyWinner.findMany({
          where: {
            AND: [{ game_id: originalOrder?.game_id }, { winning_date: originalOrder?.timestamp_created }],
          },
        })
        logger.info("ifBeenAWinner", ifBeenAWinner)

        // now check how many points to be deducted

        // now i need to check if canceledOrderCount = 1 ? -150 points
        // if count = 2 ? point = 0
        // else blacklist the player
        // then deduct
        if (canceledOrderCount && canceledOrderCount >= 3) {
          logger.info(`${originalOrder?.game_id} is blacklisted `)
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

        logger.info(`Total points for user ${originalOrder?.uid}:`, totalPoints)

        if (!originalOrder) {
          logger.info(`Original order not found for cancellation: ${orderId}`)
          throw new Error(`Original order not found for cancellation: ${orderId}`)
        }

        const {
          points: originalPoints,
          game_id: gameId,
          uid,
          last_streak_date,
          gmv: originalGmv,
          order_status,
        } = originalOrder

        logger.info("originalGmv", originalGmv), order_status

        // Function to safely parse floats and handle negative values
        const safeFloat = (value: number | number, defaultValue: number = 0): number => {
          const num = parseFloat(value.toString())
          return isNaN(num) ? defaultValue : Math.abs(num)
        }


        // Calculate new GMV
        let newGmv = safeFloat(row.total_price, 0)

        // Calculate adjustment
        let pointsAdjustment
        if (orderStatus === "cancelled") {
          newGmv = originalGmv // Full cancellation resets GMV
          pointsAdjustment = -originalPoints

          // await deductPointsForHigherSameDayOrders(uid, orderId, timestampCreated, gameId, same_day_order_count)
          
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
          logger.info("gmvAdjustment", gmvAdjustment)
        }

        logger.info("first---", timestampCreated, timestampCreated.toISOString())

        processedData.push({
          ...row,
          game_id: gameId,
          points: pointsAdjustment,
          entry_updated: true,
          streak_maintain: true,
          highest_gmv_for_day: false,
          highest_orders_for_day: false,
          gmv: newGmv,
          updated_by_lambda: new Date().toISOString(),
          timestamp_created: timestampCreated.toISOString(),
          timestamp_updated: new Date().toISOString(),
          uid: uid,
          order_status: orderStatus,
          last_streak_date,
        })
      } catch (err) {
        console.error(`Error processing cancellation for order ${row.order_id}: ${err}`)
        throw Error(`Status not active at order: ${row.order_id}: ${err}`)
        // continue
      }
    }

    return processedData
  } catch (err: any) {
    console.error(`Error processing cancellations11111: ${err}`)
    throw new Error(err.message)
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

    logger.info("highestGmvResults", highestGmvResults)

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

        logger.info(`Updating highest_gmv_for_day for game_id: ${game_id} from ${startOfDayUTC} to ${endOfDayUTC}`)

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
    logger.info("highestOrdersResults", highestOrdersResults)

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
      logger.info("order_id", order_id)
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
      }

      logger.info("total_orders", total_orders, total_gmv, max_orders, max_gmv)


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

    logger.info("Highest GMV and orders for the day updated successfully.")
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
    logger.info(game_id)
    // ? Why are we sending points + 50 in the case of originalGMV excedding 1000 & current GMV deceding 1000
    if (originalGmv > 1000 && gmv < 1000) {
      // await rewardledgerUpdate(game_id, orderId, 0, -50.0, "GMV < 1000 in partial cancellation", true, timestamp)
      return points + 50
    } else {
      points += 50
    }

    logger.info("here in partial")
    return points
  } else {
    /* empty */
  }

  if (gmv > 1000) {
    points += 50
    // await rewardledgerUpdate(game_id, orderId, 0, +50.0, "GMV Greater 1000", true, timestamp)
  }

  try {
    logger.info("==========>", timestamp)
    const orderCount = await getTodayOrderCount2(uid, timestamp, orderId)
    logger.info("==========>", orderCount, timestamp)
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
    const eligibleBonus = Math.max(
      ...Object.keys(streakBonuses)
        .map(Number)
        .filter((key) => key <= streakCount),
    )

    logger.info("eligibleBonus", eligibleBonus, streakBonuses[streakCount])

    if (streakBonuses[streakCount]) {
      points += streakBonuses[streakCount]
    }
  }

  logger.info("points", points)
  return points
}

const getTodayOrderCount2 = async (uid: string, timestamp: any, order_id: string) => {
  try {
    logger.info("timestamp2", timestamp)
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

const bulkInsertDataIntoDb = async (data: any) => {
  /**
   * Bulk inserts data into the database using Prisma.
   */
  if (!data || data.length === 0) return
  logger.info("row", JSON.stringify(data[0].uploaded_by))


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

  try {
    const formattedData = filteredData.map((row: any) => ({
      order_id: row.order_id,
      order_status: row.order_status,
      timestamp_created: row.timestamp_created,
      timestamp_updated: row.timestamp_updated,
      buyer_app_id: row.buyer_app_id,
      total_price: parseFloat(row.total_price || 0),
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
    logger.info("The inserted Data is: ", insertedData)
    logger.info(`Bulk data inserted successfully.`)
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
    logger.info("userId", userId, "Page:", page, "Limit:", limit)

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

export const rewardledger = async () => {
  try {
    const data = await prisma.rewardLedger.findMany()
    return { data }
  } catch (error) {
    console.error("❌ Error setting up rewardledger trigger:", error)
    throw new Error("Failed to fetch rewardledger")
  }
}

export const db = async () => {
  try {
    const data = await prisma.orderData.findMany()
    return { data }
  } catch (error) {
    console.error("❌ Error setting up rewardledger trigger:", error)
    throw new Error("Failed to fetch rewardledger")
  }
}

export const removetrigger = async () => {
  try {
    const data = await await prisma.$executeRawUnsafe(`DROP TRIGGER IF EXISTS rewardTrigggered ON "orderData"`)
    return { data }
  } catch (error) {
    console.error("❌ Error setting up rewardledger trigger:", error)
    throw new Error("Failed to fetch rewardledger")
  }
}

export const rewardledgertesting = async () => {
  try {
    const data = await prisma.rewardLedgerTesting.findMany()
    return { data }
  } catch (error) {
    console.error("❌ Error setting up rewardledger trigger:", error)
    throw new Error("Failed to fetch rewardledger")
  }
}

export const isDuplicateOrder = async (orderId: string, orderStatus: any, buyerAppId: string) => {
  const existingOrder = await prisma.orderData.findFirst({
    where: {
      order_id: orderId,
      order_status: orderStatus,
      buyer_app_id: buyerAppId,
    },
  })

  return existingOrder !== null
}
