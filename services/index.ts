import fs from "fs"
import csvParser from "csv-parser"
import { blake2b } from "blakejs"
import { Decimal } from "@prisma/client/runtime/library"
import { ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3"
import { Readable } from "stream"
import moment from "moment-timezone"
import { prisma } from "../prisma/index"
import { logger } from "../shared/logger"
import { FullProcessedOrderRecord, NormalizedRow, OrderRecord, cancelledOrders } from "interfaces/test"
import { s3Client } from "../shared/s3client"
import {
  validatePhoneNumber,
  validateTotalPrice,
  uploadToS3,
  saveInvalidOrdersToCSV,
  getCsvLineCount,
  validateCSVHeadersStrict,
  checkForDuplicates,
} from "../shared/utils"

export const parseAndStoreCsv = async (
  filePath: string,
  userId: number,
  buyer_name: string,
): Promise<{ success: boolean; message: string }> => {
  const records: OrderRecord[] = []
  const recordMap = new Map<string, { orderStatus: string; totalPrice: number }>()
  let rowCount = 0
  // eslint-disable-next-line no-async-promise-executor
  return new Promise(async (resolve, reject) => {
    try {
      const lines = getCsvLineCount(filePath)
      if (lines > 100000) {
        return reject({ success: false, message: "Record length exceeded 100000" })
      }

      const headerValidation = validateCSVHeadersStrict(filePath)
      if (!headerValidation.success) {
        return reject({ success: false, message: headerValidation.message })
      }
    } catch (err) {
      if (err instanceof Error) {
        return reject({ success: false, message: err.message })
      }

      return reject({ success: false, message: "Unknown error during header validation" })
    }

    const stream = fs.createReadStream(filePath).pipe(csvParser())
    let processingPromise: Promise<void> = Promise.resolve()
    stream
      .on("data", async (row: any) => {
        processingPromise = processingPromise.then(async () => {
          try {
            rowCount = rowCount + 1
            let check = false
            const emptyFields: string[] = []

            const normalizedRow = Object.fromEntries(
              Object.entries(row).map(([key, value]) => {
                const normalizedKey = key.trim().toLowerCase().replace(/\s+/g, "_")

                if (!value) {
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

            if (check) {
              return reject({
                success: false,
                message: `The following fields are empty or invalid: ${emptyFields.join(", ")} at index:${rowCount}`,
              })
            }

            //order status checks, valid status, duplicate status
            const orderId: string = normalizedRow["order_id"]
            const orderStatus: string = String(normalizedRow["order_status"])?.toLowerCase()
            const totalPrice: number = parseFloat(String(normalizedRow["total_price"]))
            const timestampStr: string = normalizedRow["timestamp_created"]
            const timestampCreated: Date = moment
              .tz(timestampStr, "YYYY-MM-DD HH:mm:ss", "Asia/Kolkata")
              .add(5, "hours")
              .add(30, "minutes")
              .toDate()
            const validOrderStatus = ["active", "partially_cancelled", "cancelled"]
            if (!validOrderStatus.includes(orderStatus))
              return reject({
                success: false,
                message: `issue with order status at index:${rowCount}`,
              })

            const duplicateCheck = await checkForDuplicates(orderId, orderStatus, String(userId), recordMap, totalPrice)

            if (!duplicateCheck.success) {
              return reject({
                success: false,
                message: duplicateCheck.message,
              })
            }

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
              timestamp_updated: new Date(String(normalizedRow["timestamp_updated"])) || timestampCreated,
              buyer_app_id: String(userId),
              buyer_name: String(buyer_name),
              total_price: totalPrice,
            })

            logger.info("records", records)

            recordMap.set(orderId as string, {
              orderStatus: normalizedRow["order_status"] as string,
              totalPrice: normalizedRow["total_price"] as number
            })
          } catch (error: any) {
            logger.info("error", error)
            stream.destroy()
            return reject({ success: false, message: error.message })
          }
        })
      })
      .on("end", async () => {
        try {
          // if (shouldAbort) return
          await processingPromise
          if (records.length === 0) {
            logger.info("parsed records", records)
            logger.info("⚠️ No valid records found in the CSV file")
            return resolve({
              success: false,
              message: "No valid records found in the CSV file",
            })
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

          uploadToS3(filePath, buyer_name, String(userId)).then((response) => {
            logger.info(`result of upload to bucket ${response.success} and ${response.url}`)
          })

          let processedOrders: any = []
          if (newOrders.length > 0) {
            const response = await processNewOrders(newOrders)
            if (!response?.error) processedOrders = response.processedData
            else return reject({ success: false, message: response?.message })
          }

          if (cancellations.length > 0) {
            const response = await processCancellations(cancellations)
            if (!response?.error) processedOrders = processedOrders.concat(response.processedData)
            else return reject({ success: false, message: response?.message })
          }

          if (processedOrders.length > 0) {
            try {
              await bulkInsertDataIntoDb(processedOrders)
            } catch (error: any) {
              logger.info("Error while storing non-active orders", error)
              throw new Error(error.message)
            }
          }

          logger.info("✅ CSV data stored successfully")
          resolve({ success: true, message: "CSV data stored successfully" })
        } catch (error: any) {
          logger.error("❌ Error storing CSV data:", error)
          reject({
            success: false,
            message: "Error storing CSV data: " + error.message,
          })
        } finally {
          fs.unlinkSync(filePath)
        }
      })
      .on("error", (error) => {
        logger.error("❌ Error reading CSV file:", error)
        reject({ success: false, message: "Error reading CSV file: " + error })
      })
  })
}

export const search = async (game_id: string, format: string) => {
  try {
    logger.info(`Format: ${format}, "Game ID:", ${game_id}`)

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

    logger.info(`Start Date Filter (UTC): ${startDate.toISOString()}`)

    const totalPoints = await prisma.$queryRaw`
  SELECT COALESCE(SUM(points), 0) AS total_points, game_id
  FROM rewardledgertesting
  WHERE game_id LIKE ${game_id} || '%'
  AND created_at >= ${new Date(startDate).toISOString()}::timestamp AT TIME ZONE 'UTC'
  GROUP BY game_id
`

    logger.info("Total Points Result:", totalPoints)
    return totalPoints
  } catch (error) {
    logger.error("Error in search function:", error)
    throw error
  }
}

const processNewOrders = async (orders: any) => {
  const processedData = []
  try {
    const uidFirstOrderTimestamp: any = {}

    for (const row of orders) {
      try {
        const uid = String(row.uid || "").trim()
        const timestampCreated: Date = row.timestamp_created

        // Get existing user data
        const existingUser = await prisma.orderData.findFirst({
          where: { uid: uid },
          orderBy: { timestamp_created: "desc" },
          select: { game_id: true, last_streak_date: true, streak_count: true },
        })

        let game_id, lastStreakDate
        // streakCount = 1
        // let phone_number
        if (existingUser) {
          game_id = existingUser.game_id
          lastStreakDate = existingUser.last_streak_date || timestampCreated
          // streakCount = existingUser.streak_count
        } else {
          if (!uidFirstOrderTimestamp[uid]) {
            uidFirstOrderTimestamp[uid] = String(new Date(timestampCreated).getUTCHours()).padStart(2, "0")
          }

          //GAME ID FORMATION
          lastStreakDate = timestampCreated
          const hours = String(new Date(timestampCreated).getUTCHours())
          const minutes = String(new Date(timestampCreated).getUTCMinutes())
          const result = uid.slice(3, 11) + hours + minutes
          const temp_id = `${result}`
          const hash = blake2b(temp_id, undefined, 64)
          const hashedId = Buffer.from(hash).toString("hex")
          game_id = hashedId

          logger.info(`The GameID is: ${game_id}`)
        }

        logger.info(lastStreakDate)
        const orderCount = await getTodayOrderCount(uid, timestampCreated, row.order_id)
        processedData.push({
          ...row,
          // uid:phone_number,
          game_id,
          entry_updated: true,
          same_day_order_count: orderCount + 1,
          streak_maintain: true,
          highest_gmv_for_day: false,
          highest_orders_for_day: false,
          streak_count: 0,
          last_streak_date: new Date().toISOString(),
          timestamp_created: new Date(timestampCreated).toISOString(),
          timestamp_updated: new Date().toISOString(),
          uid: uid,
        })

      } catch (err: any) {
        logger.error(`Error processing new order: ${err.message}`)
        return {
          error: true,
          message: `Error processing new order: ${row.order_id}: ${err.message}`,
          orders: [],
        }
      }
    }

    return { error: false, processedData }
  } catch (err: any) {
    logger.error(`Error processing new orders: ${err}`)
    return {
      error: true,
      message: `Error processing new orders: ${err.message}`,
      orders: [],
    }
  }
}

const processCancellationRow = async (row: any) => {
  try {
    const orderId = row.order_id
    const orderStatus = (row.order_status || "").toLowerCase()
    const timestampCreated: Date = row.timestamp_created
    const possibleOrders = await prisma.orderData.findMany({
      where: {
        order_id: orderId,
        order_status: {
          in: ["partially_cancelled", "active"],
        },
      },
      orderBy: {
        timestamp_created: "desc",
      },
      take: 1,
    })

    const originalOrder = possibleOrders[0]
    // const originalOrder =
    //   possibleOrders.find((o) => o.order_status === "partially_cancelled") ||
    //   possibleOrders.find((o) => o.order_status === "active") ||
    //   null
    logger.info("originalOrder in cancellation", originalOrder)

    if (!originalOrder) {
      logger.info(`Original order not found for cancellation: ${orderId}`)
      throw new Error(`Original order not found for cancellation: ${orderId}`)
    }

    const {
      game_id: gameId,
      uid,
      last_streak_date,
      //  gmv: originalGmv, order_status
    } = originalOrder

    return {
      ...row,
      game_id: gameId,
      entry_updated: true,
      streak_maintain: true,
      highest_gmv_for_day: false,
      highest_orders_for_day: false,
      // updated_by_lambda: new Date().toISOString(),
      timestamp_created: timestampCreated.toISOString(),
      timestamp_updated: new Date().toISOString(),
      uid: uid,
      order_status: orderStatus,
      last_streak_date,
    }
  } catch (err) {
    logger.error(`Error processing cancellation for order ${row.order_id}: ${err}`)
    throw Error(`Error processing cancellation for order ${row.order_id}: ${err}`)
  }
}

const processCancellations = async (cancellations: OrderRecord[]): Promise<FullProcessedOrderRecord[] | any> => {
  const processedData = []
  logger.info("Showing Cancellation orders!")

  const partiallyCancelled = cancellations.filter((row) => row.order_status === "partially_cancelled")
  const cancelled = cancellations.filter((row) => row.order_status === "cancelled")
  try {
    for (const row of partiallyCancelled) {
      try {
        const data = await processCancellationRow(row)
        processedData.push(data)
      } catch (err) {
        logger.error(`Error processing partial cancellation for order ${row.order_id}:`, err)
        return {
          error: true,
          message: `Error processing partial cancellation for order ${row.order_id}: ${err}`,
          orders: [],
        }
      }
    }

    for (const row of cancelled) {
      try {
        const data = await processCancellationRow(row)
        processedData.push(data)
      } catch (err) {
        logger.error(`Error processing cancellation for order ${row.order_id}:`, err)
        return {
          error: true,
          message: `Error processing cancellation for order ${row.order_id}: ${err}`,
          orders: [],
        }
      }
    }

    return { error: false, processedData }
  } catch (err: any) {
    logger.error(`Error processing cancellations: ${err}`)
    return {
      error: true,
      message: `Error processing cancellations orders: ${err}`,
      orders: [],
    }
  }
}

export const calculatePoints = async (
  game_id: string,
  gmv: number,
  uid: string,
  streakCount: number,
  condition: string,
  timestamp: Date,
  originalGmv: number,
  orderId: string,
) => {
  gmv = Math.max(0, parseFloat(gmv.toString()))

  let points = 0
  const gmvPoints = Math.floor(gmv / 10)
  points += gmvPoints
  points += 10

  if (condition === "partial") {
    logger.info(game_id)
    if (originalGmv > 1000 && gmv < 1000) {
      return points + 50
    } else {
      points += 50
    }

    logger.info("here in partial")
    return points
  }

  if (gmv > 1000) {
    points += 50
  }

  try {
    const orderCount = await getTodayOrderCount(uid, timestamp, orderId)
    points += orderCount * 5
  } catch (error) {
    logger.error(`Error calculating order count points for ${uid}:`, error)
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

    if (streakBonuses[streakCount]) {
      points += streakBonuses[streakCount]
    }
  }

  return points
}

const getTodayOrderCount = async (uid: string, timestamp: Date, order_id: string) => {
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
    logger.error(`Error fetching order count for ${uid}:`, error)
    return 0
  }
}

const bulkInsertDataIntoDb = async (data: any) => {
  if (!data || data.length === 0) return
  logger.info("row", JSON.stringify(data[0].buyer_app_id))

  try {
    const insertedData = await prisma.orderData.createMany({
      data: data,
    })
    logger.info("The inserted Data is: ", insertedData)
    logger.info(`Bulk data inserted successfully.`)
  } catch (error: any) {
    logger.error(`Error inserting bulk data`, error)
    const message = error?.meta?.message || error?.message
    if (message) {
      const errCodeIndex = message.indexOf("ERR_CODE:")
      if (errCodeIndex !== -1) {
        const extractedMessage = message.slice(errCodeIndex)
        let temp = `${extractedMessage}`
        temp = temp.split(":")[2].split(",")[0]
        throw new Error(temp)
      } else {
        logger.error("Error Message:", message)
      }
    }
  }
}

export const getUserOrders = async (userId: number, page: number = 1, limit: number = 10) => {
  try {
    logger.info("userId", userId, "Page:", page, "Limit:", limit)

    const skip = (page - 1) * limit

    const orders = await prisma.orderData.findMany({
      where: { buyer_app_id: String(userId) },
      orderBy: { timestamp_created: "desc" },
      skip,
      take: limit,
    })

    const totalOrders = await prisma.orderData.count({
      where: { buyer_app_id: String(userId) },
    })

    return { orders, totalOrders }
  } catch (error) {
    logger.error("Error fetching user orders:", error)
    throw new Error("Failed to fetch user orders")
  }
}

export const getUserOrdersForCSV = async (userId: number) => {
  try {
    const orders = await prisma.orderData.findMany({
      where: { buyer_app_id: String(userId) },
      orderBy: { timestamp_created: "desc" },
    })

    return { orders }
  } catch (error) {
    logger.error("Error fetching user orders:", error)
    throw new Error("Failed to fetch user orders")
  }
}

export const db = async () => {
  try {
    const data = await prisma.orderData.findMany()
    return { data }
  } catch (error) {
    logger.error("❌ Error setting up rewardledger trigger:", error)
    throw new Error("Failed to fetch rewardledger")
  }
}

export const removetrigger = async () => {
  try {
    const data = await await prisma.$executeRawUnsafe(`DROP TRIGGER IF EXISTS rewardTrigggered ON "orderData"`)
    return { data }
  } catch (error) {
    logger.error("❌ Error setting up rewardledger trigger:", error)
    throw new Error("Failed to fetch rewardledger")
  }
}

export const rewardledgertesting = async () => {
  try {
    const data = await prisma.rewardLedgerTesting.findMany()
    return { data }
  } catch (error) {
    logger.error("❌ Error setting up rewardledger trigger:", error)
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
    logger.error("error at inserting in rewardledgertesting", error)
    throw new Error("failed to Insert in rewardledger")
  }
}

export async function listTodayFiles() {
  const prefix = "uploads/"
  const today = new Date()
  today.setHours(0, 0, 0, 0)
  const startOfToday = today.getTime()
  const endOfToday = startOfToday + 86400000 // 1 day in ms

  const command = new ListObjectsV2Command({
    Bucket: process.env.AWS_S3_BUCKET_NAME,
    Prefix: prefix,
  })

  const result = await s3Client.send(command)
  const allTodayFiles =
    result.Contents?.filter((obj) => {
      const key = obj.Key || ""
      const parts = key.split("/")
      const timestampStr = parts[2] // uploads/{buyer_app_id}/{timestamp}/...
      const timestamp = parseInt(timestampStr, 10)
      return timestamp >= startOfToday && timestamp < endOfToday
    }) || []

  return allTodayFiles.map((file) => {
    const parts = file.Key!.split("/")
    return {
      key: file.Key!,
      buyer_app: parts[1],
      buyer_app_id: parts[2], // uploads/{buyer_app_id}/...
    }
  })
}

async function streamToString(stream: Readable): Promise<string> {
  const chunks: Uint8Array[] = []
  for await (const chunk of stream) {
    chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk)
  }

  return Buffer.concat(chunks).toString("utf-8")
}

export async function getTodayFileContents() {
  const keys = await listTodayFiles()
  const contents: { key: string; content: string; buyer_app: string; buyer_app_id: string }[] = []

  for (const { key, buyer_app, buyer_app_id } of keys) {
    const command = new GetObjectCommand({
      Bucket: process.env.AWS_S3_BUCKET_NAME,
      Key: key,
    })

    const response = await s3Client.send(command)
    const body = response.Body as Readable

    const fileContent = await streamToString(body)
    contents.push({ key, content: fileContent, buyer_app, buyer_app_id })
  }

  return contents
}

export async function getTodayFileContentsWithValidation() {
  const files = await getTodayFileContents()
  const results: {
    key: string
    ordersInFile: Record<string, string>[]
    validOrders: Record<string, string>[]
    invalidOrders: Record<string, string>[]
  }[] = []

  for (const file of files) {
    const orders = extractOrdersFromCSV(file.content)
    const orderIds = orders.map((order) => order["Order ID"])
    const validOrderSet = await getValidOrderIds(orderIds)

    const valid = orders.filter((order) => validOrderSet.has(order["Order ID"]))
    const invalid = orders.filter((order) => !validOrderSet.has(order["Order ID"]))

    if (invalid.length > 0) {
      const invalidFilePath = `/tmp/invalid_orders_${file.key.replace("/", "_")}.csv`

      const { success, filePath } = await saveInvalidOrdersToCSV(invalid, invalidFilePath)
      if (success) {
        // const result = await parseAndStoreCsv(filePath, file.buyer_app_id, file.buyer_app)
        logger.info("parseAndStoreCsv result for invalids:")
      } else {
        logger.info("Could not save invalid orders as CSV:", filePath)
      }
    }

    results.push({
      key: file.key,
      ordersInFile: orders,
      validOrders: valid,
      invalidOrders: invalid,
    })
  }

  return results
}

async function getValidOrderIds(orderIds: string[]): Promise<Set<string>> {
  const found = await prisma.orderData.findMany({
    where: {
      order_id: {
        in: orderIds,
      },
    },
    select: { order_id: true },
  })
  return new Set(found.map((o) => o.order_id))
}

function extractOrdersFromCSV(content: string): Record<string, string>[] {
  const lines = content.split("\n").filter(Boolean)
  const headers = lines[0].split(",").map((h) => h.trim())
  const dataLines = lines.slice(1)

  return dataLines
    .map((line) => {
      const values = line.split(",").map((val) => val.trim())
      if (values.length !== headers.length) return null

      const row: Record<string, string> = {}
      headers.forEach((header, index) => {
        row[header] = values[index]
      })

      return row
    })
    .filter((row): row is Record<string, string> => row !== null)
}

