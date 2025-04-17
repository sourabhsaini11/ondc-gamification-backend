import fs from "fs"
import csvParser from "csv-parser"
import { PrismaClient } from "@prisma/client"
import moment from "moment-timezone"
import { logger } from "../shared/logger"
import { blake2b } from "blakejs"
import { Decimal } from "@prisma/client/runtime/library"
// import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const prisma = new PrismaClient()

type NormalizedRow = {
  order_id: string
  order_status: string
  timestamp_created: string
  total_price: number
  phone_number: string
  timestamp_updated?: string
}
type OrderRecord = {
  uid: string
  order_id: string
  order_status: string
  timestamp_created: Date
  timestamp_updated: Date
  buyer_app_id?: string
  buyer_name: string
  total_price: number
  uploaded_by: number
}
type FullProcessedOrderRecord = Omit<
  OrderRecord,
  "timestamp_created" | "timestamp_updated" | "uid" | "order_status"
> & {
  uid: string
  order_id: string
  order_status: string
  timestamp_created: string
  timestamp_updated: string
  game_id: string
  points: number
  entry_updated: boolean
  streak_maintain: boolean
  highest_gmv_for_day: boolean
  highest_orders_for_day: boolean
  same_day_order_count?: number
  streak_count?: number
  gmv: number
  updated_by_lambda: string
  last_streak_date: any
}
type OrderStatusValidationResult = { success: boolean; message?: string }
type cancelledOrders = { order_id: string }

export const findInvalidOrderStatus = (orders: OrderRecord[]): OrderStatusValidationResult => {
  const validStatuses = new Set(["active", "created", "partially_cancelled", "cancelled"])

  for (const order of orders) {
    if (!validStatuses.has(order.order_status)) {
      return {
        success: false,
        message: `Invalid order_status = ${order.order_status} for order_id: ${order.order_id}`,
      }
    }
  }

  return { success: true }
}

export const findDuplicateOrderIdAndStatys = (orders: { order_id: string; order_status: string }[]) => {
  const seen = new Set<string>()
  for (const order of orders) {
    const key = `${order.order_id}-${order.order_status}`

    if (seen.has(key)) {
      // Allow duplicates only if it's 'partially_cancelled'
      if (order.order_status !== "partially_cancelled") {
        return {
          success: false,
          message: `Duplicate found: Order ID ${order.order_id} with status ${order.order_status}`,
        }
      }

      // else skip error for 'partially_cancelled'
      continue
    }

    seen.add(key)
  }

  return { success: true, message: "No duplicates found" }
}

const validatePhoneNumber = (phone_number: any, index: number): OrderStatusValidationResult => {
  const phoneRegex = /^\d{3}XXX\d{4}$/ // Expected format: 733XXX1892

  if (!/^\d{10}$/.test(phone_number.replace(/X/g, "0"))) {
    return { success: false, message: `Invalid phone number at row: ${index}` }
  }

  if (!phoneRegex.test(phone_number)) {
    return {
      success: false,
      message: `Masking of phone number not followed for row: ${index}`,
    }
  }

  if (/[^0-9X]/.test(phone_number)) {
    return {
      success: false,
      message: `Invalid phone number format at row: ${index}}`,
    }
  }

  return { success: true }
}

const validateTotalPrice = (total_price: any, index: number): OrderStatusValidationResult => {
  logger.info("total_price", total_price)
  if (typeof total_price !== "number" || isNaN(total_price) || /[^0-9.]/.test(total_price.toString())) {
    return { success: false, message: `Invalid total price at row ${index}` }
  }

  if (total_price <= 0) {
    return {
      success: false,
      message: `Issue with total price at row ${index}`,
    }
  }

  return { success: true }
}
const validateOrderTimestamp = (orders: OrderRecord[]): OrderStatusValidationResult => {
  const now = new Date()
  const oneDayLater = new Date()
  oneDayLater.setDate(now.getDate() + 1) // Allow timestamps up to 1 day in the future

  for (const order of orders) {
    const { order_id, timestamp_created: timestamp } = order

    // Try to parse the timestamp
    const orderDate = new Date(timestamp)

    // Check if the parsed date is invalid
    if (isNaN(orderDate.getTime())) {
      return {
        success: false,
        message: `timestamp error at order_id ${order_id}`,
      }
    }

    // Ensure it's not more than 1 day in the future
    if (orderDate > oneDayLater) {
      return {
        success: false,
        message: `future timestamp error at order_id ${order_id}`,
      }
    }
  }

  return { success: true }
}

export const parseAndStoreCsv = async (
  filePath: string,
  userId: number,
  buyer_name: string,
  // originalName: string
): Promise<{ success: boolean; message: string }> => {
  const records: OrderRecord[] = []
  const recordMap = new Map<string, { orderStatus: string; buyerAppId: string }>()
  const partialMap = new Map<string, { orderStatus: string; buyerAppId: string }>()
  let rowCount = 0

  return new Promise((resolve, reject) => {
    const shouldAbort = false
    const stream = fs.createReadStream(filePath).pipe(csvParser())
    stream
      .on("data", (row: any) => {
        try {
          rowCount++
          if (rowCount > 100000) {
            return reject({
              success: false,
              message: "Record length exceeded 100000",
            })
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
          ) as NormalizedRow
          console.log("normalizedrow", normalizedRow)
          if (check) {
            console.error("Values can't be empty")
            return reject({
              success: false,
              message: `The following fields are empty or invalid: ${emptyFields.join(", ")} at index:${rowCount}`,
            })
          }

          const requiredFields: (keyof NormalizedRow)[] = [
            "phone_number",
            "order_id",
            "order_status",
            "timestamp_created",
            "total_price",
          ]

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

          const extraFields = rowKeys.filter((key) => !requiredFields.includes(key as keyof NormalizedRow))
          if (extraFields.length > 0) {
            console.error(`❌ Unexpected Fields: ${extraFields.join(", ")}`)
            return reject({
              success: false,
              message: `Unexpected fields found: ${extraFields.join(", ")}`,
            })
          }

          const orderId: string = normalizedRow["order_id"]
          const existingRecord = recordMap.get(orderId as string)
          const orderStatus: string = String(normalizedRow["order_status"])?.toLowerCase()
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

            const activeRecord: OrderRecord | undefined = records?.find(
              (record) => record.order_id === orderId && record.order_status === "active",
            )

            if (activeRecord) {
              if (normalizedRow["total_price"] > activeRecord?.total_price) {
                return reject({
                  success: false,
                  message: `
                  GMV greater than active order GMV at index:${rowCount} for Order ID ${orderId}`,
                })
              }
            }
          }


          const timestampStr: string = normalizedRow["timestamp_created"] // Example: "2025-02-24 2:00:00"
          const totalPrice: number = parseFloat(String(normalizedRow["total_price"]))
          const timestampCreated: Date = moment
            .tz(timestampStr, "YYYY-MM-DD HH:mm:ss", "Asia/Kolkata")
            .add(5, "hours")
            .add(30, "minutes")
            .toDate()

          if (isNaN(timestampCreated.getTime())) {
            logger.info(`Invalid timestamp for order ${orderId}`)
            return reject({
              success: false,
              message: `Invalid timestamp for order ${orderId} at index:${rowCount}`,
            })
          }

          // check for invalid total_price
          const isInvalidTotalPrice = validateTotalPrice(totalPrice, rowCount)
          if (!isInvalidTotalPrice.success) {
            return reject({
              success: false,
              message: isInvalidTotalPrice.message,
            })
          }

          // check for invalid phone_number
          const isInvalidPhoneNumber = validatePhoneNumber(normalizedRow["phone_number"], rowCount)
          if (!isInvalidPhoneNumber.success) {
            return reject({
              success: false,
              message: isInvalidPhoneNumber.message,
            })
          }

          records.push({
            uid: String(normalizedRow["phone_number"])?.trim(),
            order_id: orderId,
            order_status: orderStatus,
            timestamp_created: timestampCreated,
            timestamp_updated: new Date(String(normalizedRow["timestamp_updated"])) || timestampCreated, // timestamp_Updated update
            buyer_app_id: String(userId),
            buyer_name: String(buyer_name),
            total_price: totalPrice,
            uploaded_by: userId,
          })

          logger.info("records12", records)

          // Store order_id with its order_status in Map
          recordMap.set(orderId as string, {
            orderStatus: normalizedRow["order_status"] as string,
            buyerAppId: String(userId),
          })
          partialMap.set(orderId as string, {
            orderStatus: normalizedRow["order_status"] as string,
            buyerAppId: String(userId),
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
            return resolve({
              success: false,
              message: "No valid records found in the CSV file",
            })
          }

          for (const row of records) {
            if (await isDuplicateOrder(row.order_id, row.order_status, row.buyer_app_id || "")) {
              return reject({
                success: false,
                message: `Duplicate order ${row.order_id} (${row.order_status}) at row ${rowCount}`,
              })
            }

            if (row.order_status === "cancelled" || row.order_status === "partially_cancelled") {
              const existingOrder = await prisma.orderData.findFirst({
                where: {
                  order_id: row.order_id,
                  order_status: "active",
                  buyer_app_id: row.buyer_app_id,
                },
              })

              if (existingOrder && row.total_price > existingOrder.total_price) {
                return reject({
                  success: false,
                  message: `GMV greater than active order GMV for Order ID ${row.order_id}`,
                })
              }
            }
          }

          const newOrders: OrderRecord[] = []
          const cancellations: OrderRecord[] = []

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
            return reject({
              success: false,
              message: isInvalidTimestamp.message,
            })
          }

          // check for invalid order_status
          const isInvalidOrderStatus: OrderStatusValidationResult = findInvalidOrderStatus([
            ...newOrders,
            ...cancellations,
          ])
          if (!isInvalidOrderStatus.success) {
            return reject({
              success: false,
              message: isInvalidOrderStatus.message,
            })
          }

          // check for duplicate order id for same status
          const isDuplicateNewOrder = findDuplicateOrderIdAndStatys(newOrders)
          const isDuplicateCancellation = findDuplicateOrderIdAndStatys(cancellations)
          if (!isDuplicateNewOrder.success) {
            return reject({
              success: false,
              message: isDuplicateNewOrder.message,
            })
          }

          if (!isDuplicateCancellation.success) {
            return reject({
              success: false,
              message: isDuplicateCancellation.message,
            })
          }

          if (newOrders.length > 0) {
            const processedNewOrders: FullProcessedOrderRecord[] = await processNewOrders(newOrders)
            await bulkInsertDataIntoDb(processedNewOrders)
          }

          if (cancellations.length > 0) {
            try {
              const processedCancelOrders: FullProcessedOrderRecord[] = await processCancellations(cancellations)
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
          reject({
            success: false,
            message: "Error storing CSV data: " + error.message,
          })
        } finally {
          // fs.unlinkSync(filePath)
          await prisma.$disconnect()
        }
      })
      .on("error", (error) => {
        console.error("❌ Error reading CSV file:", error)
        reject({ success: false, message: "Error reading CSV file: " + error })
      })
  })
}

export const search = async (game_id: string, format: string) => {
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

const processNewOrders = async (orders: OrderRecord[]) => {
  const processedData = []

  try {
    const uidFirstOrderTimestamp: any = {}

    for (const row of orders) {
      try {
        const uid = String(row.uid || "").trim()
        const timestampCreated: Date = row.timestamp_created
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

          // const fullUid = uid

          lastStreakDate = timestampCreated
          //GAME ID FORMATION
          // const timestamp = timestampCreated
          const hours = String(new Date(timestampCreated).getUTCHours()) // Ensures valid ISO format
          const minutes = String(new Date(timestampCreated).getUTCMinutes())
          console.log("uid", uid, "hours", hours, "minutes", minutes)
          const result = uid.slice(3, 11) + hours + minutes
          console.log("result", result)
          const temp_id = `${result}`
          const hash = blake2b(temp_id, undefined, 64) // 64-byte (512-bit) hash
          const hashedId = Buffer.from(hash).toString("hex")
          game_id = hashedId

          logger.info(`The GameID is: ${game_id}`)
        }

        logger.info(lastStreakDate)
        // Calculate GMV
        const gmv = Number(row.total_price) || 0

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
        const orderCount = await getTodayOrderCountNew(uid, timestampCreated, row.order_id)

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

const processCancellations = async (cancellations: OrderRecord[]): Promise<FullProcessedOrderRecord[]> => {
  const processedData = []
  logger.info("Showing Cancellation orders!")
  logger.info(cancellations)

  const partiallyCancelled = cancellations.filter((row) => row.order_status === "partially_cancelled")
  const cancelled = cancellations.filter((row) => row.order_status === "cancelled")
  try {
    for (const row of partiallyCancelled) {
      try {
        const orderId = row.order_id
        const orderStatus = (row.order_status || "").toLowerCase()
        const timestampCreated: Date = row.timestamp_created

        const possibleOrders = await prisma.orderData.findMany({
          where: {
            order_id: orderId,
            order_status: {
              in: ["partially_cancelled", "active"], // include both
            },
          },
          orderBy: {
            timestamp_created: "desc",
          },
        })
        const originalOrder =
          possibleOrders.find((o) => o.order_status === "partially_cancelled") ||
          possibleOrders.find((o) => o.order_status === "active") ||
          null
        logger.info("originalOrder in cancellation", originalOrder)

        const totalPoints =
          originalOrder &&
          (await prisma.orderData.groupBy({
            by: ["uid"], // Group by user ID
            _sum: {
              points: true, // Sum the points for each user
            },
            where: {
              uid: originalOrder.uid, // Filter for the specific user
              order_status: "active", // Only consider orders with status 'created'
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
        const newGmv = safeFloat(row.total_price, 0)

        // Calculate adjustment
        let pointsAdjustment

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

        // eslint-disable-next-line prefer-const
        pointsAdjustment = newPoints - originalPoints // 110 - 210 = -110
        const gmvAdjustment = originalGmv - newGmv
        logger.info("gmvAdjustment", gmvAdjustment)

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

    for (const row of cancelled) {
      try {
        const orderId = row.order_id
        const orderStatus = (row.order_status || "").toLowerCase()
        const timestampCreated: Date = row.timestamp_created

        const possibleOrders = await prisma.orderData.findMany({
          where: {
            order_id: orderId,
            order_status: {
              in: ["partially_cancelled", "active"], // include both
            },
          },
          orderBy: {
            timestamp_created: "desc",
          },
        })
        const originalOrder =
          possibleOrders.find((o) => o.order_status === "partially_cancelled") ||
          possibleOrders.find((o) => o.order_status === "active") ||
          null
        logger.info("originalOrder in cancellation", originalOrder)

        const totalPoints =
          originalOrder &&
          (await prisma.orderData.groupBy({
            by: ["uid"], // Group by user ID
            _sum: {
              points: true, // Sum the points for each user
            },
            where: {
              uid: originalOrder.uid, // Filter for the specific user
              order_status: "active", // Only consider orders with status 'created'
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
        newGmv = originalGmv // Full cancellation resets GMV

        // eslint-disable-next-line prefer-const
        pointsAdjustment = -originalPoints

        // await deductPointsForHigherSameDayOrders(uid, orderId, timestampCreated, gameId, same_day_order_count)

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

    console.log("ProcessedData here", processedData)
    return processedData
  } catch (err: any) {
    console.error(`Error processing cancellations11111: ${err}`)
    throw new Error(err.message)
  }
}

const calculatePoints = async (
  game_id: string,
  gmv: number,
  uid: string,
  streakCount: number,
  condition: string,
  timestamp: Date,
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
  }

  if (gmv > 1000) {
    points += 50
    // await rewardledgerUpdate(game_id, orderId, 0, +50.0, "GMV Greater 1000", true, timestamp)
  }

  try {
    logger.info("==========>", timestamp)
    const orderCount = await getTodayOrderCountNew(uid, timestamp, orderId)
    logger.info("==========>", orderCount, timestamp)
    points += orderCount * 5
  } catch (error) {
    console.error(`Error calculating order count points for ${uid}:`, error)
  }

  if (streakCount > 0) {
    const streakBonuses: Record<number, number> = {
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

const getTodayOrderCountNew = async (uid: string, timestamp: Date, order_id: string) => {
  try {
    logger.info("timestamp2", timestamp)
    const startOfDay = new Date(timestamp)
    startOfDay.setHours(0, 0, 0, 0)

    const endOfDay = new Date(timestamp)
    endOfDay.setHours(23, 59, 59, 999)

    // Get all order_id values that have at least one "cancelled" order
    const cancelledOrders: cancelledOrders[] = await prisma.orderData.findMany({
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

    const cancelledOrderIds = cancelledOrders.map((order: cancelledOrders) => order.order_id)

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

const bulkInsertDataIntoDb = async (data: FullProcessedOrderRecord[]) => {
  /**
   * Bulk inserts data into the database using Prisma.
   */
  if (!data || data.length === 0) return
  logger.info("row", JSON.stringify(data[0].uploaded_by))

  try {
    const formattedData: FullProcessedOrderRecord[] = data.map((row: FullProcessedOrderRecord) => ({
      order_id: row.order_id,
      order_status: row.order_status,
      timestamp_created: row.timestamp_created,
      timestamp_updated: row.timestamp_updated,
      buyer_app_id: row.buyer_app_id,
      buyer_name: row.buyer_name,
      total_price: Number(row.total_price || 0),
      uid: row.uid,
      game_id: row.game_id,
      points: Number(row.points || 0),
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

    const insertedData = await prisma.orderData.createMany({
      data: formattedData,
    })
    logger.info("The inserted Data is: ", insertedData)
    logger.info(`Bulk data inserted successfully.`)
  } catch (error: any) {
    console.error(`Error inserting bulk data`, error)

    const message = error?.meta?.message || error?.message

    if (message) {
      // Optional: You could extract specifically the part that starts with "ERR_CODE:"
      const errCodeIndex = message.indexOf("ERR_CODE:")
      if (errCodeIndex !== -1) {
        const extractedMessage = message.slice(errCodeIndex)
        console.error("Extracted Error:", extractedMessage)
        let temp = `${extractedMessage}`
        console.log("temp", temp)
        temp = temp.split(":")[2].split(",")[0]
        throw new Error(temp)
      } else {
        console.error("Error Message:", message)
      }
    }
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

export const downloadleaderboard = async (type: string) => {
  try {
    let result
    if (type === "daily_top_leaderboard") {
      result = await prisma.$queryRaw` Select * from daily_top_leaderboard`
    } else if (type === "weekly_top_leaderboard") {
      result = await prisma.$queryRaw` Select * from weekly_top_leaderboard`
    } else {
      result = await prisma.$queryRaw` Select * from monthly_top_leaderboard`
    }

    const cleanResult = convertBigIntToString(result)
    return { result: cleanResult }
  } catch (error) {
    console.log(error)
    throw new Error("failed to fetch leaderboard")
  }
}

const convertBigIntToString = (obj: any): any => {
  if (Array.isArray(obj)) {
    return obj.map(convertBigIntToString)
  } else if (obj && typeof obj === "object") {
    return Object.fromEntries(Object.entries(obj).map(([key, value]) => [key, convertBigIntToString(value)]))
  } else if (typeof obj === "bigint") {
    return obj.toString()
  } else if (obj instanceof Decimal) {
    return obj.toNumber()
  } else {
    return obj
  }
}

export const insertrewardledgertesting = async (
  game_id: string,
  order_id: string,
  gmv: number,
  points: number,
  reason: string,
  order_status: string,
  order_timestamp_created: Date,
) => {
  try {
    logger.info("Inserting in Rewardledgertesting")
    const result = await prisma.rewardLedgerTesting.create({
      data: {
        game_id: game_id,
        order_id: order_id,
        gmv: gmv,
        points: points,
        reason: reason,
        order_status: order_status,
        order_timestamp_created: order_timestamp_created,
      },
    })
    return { result }
  } catch (error) {
    console.error("error at inserting in rewardledgertesting", error)
    throw new Error("failed to Insert in rewardledger")
  }
}
