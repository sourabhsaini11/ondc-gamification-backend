import fs from "fs"
import { Upload } from "@aws-sdk/lib-storage"
import { Parser } from "json2csv"
import path from "path"
import { execSync } from "child_process"
import { IUser, OrderStatusValidationResult, OrderRecord } from "../interfaces/test"
import { logger } from "../shared/logger"
import { s3Client } from "../shared/s3client"
import { requiredFields } from "../constants"
import { prisma } from "../prisma/index"

export const getUser = (): IUser => {
  return { name: "Test User", age: 24, gender: "male" }
}

export const validatePhoneNumber = (phone_number: string, index: number): OrderStatusValidationResult => {
  const phoneRegex = /^\d{3}XXX\d{4}$/

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

export const validateTotalPrice = (total_price: number, index: number): OrderStatusValidationResult => {
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

export const validateOrderTimestamp = (orders: OrderRecord[]): OrderStatusValidationResult => {
  const now = new Date()
  const oneDayLater = new Date()
  oneDayLater.setDate(now.getDate() + 1) 

  for (const order of orders) {
    const { order_id, timestamp_created: timestamp } = order

    const orderDate = new Date(timestamp)

    if (isNaN(orderDate.getTime())) {
      return {
        success: false,
        message: `timestamp error at order_id ${order_id}`,
      }
    }

    if (orderDate > oneDayLater) {
      return {
        success: false,
        message: `future timestamp error at order_id ${order_id}`,
      }
    }
  }

  return { success: true }
}

export const uploadToS3 = async (
  filePath: string,
  buyer_name: string,
  buyer_id: string,
): Promise<{ success: boolean; url?: string; message?: string }> => {
  const fileStream = fs.createReadStream(filePath)
  const folder = `uploads/${buyer_name}/${buyer_id}`
  const fileName = `${folder}/${Date.now()}`

  const uploadParams = {
    Bucket: process.env.AWS_S3_BUCKET_NAME!,
    Key: fileName,
    Body: fileStream,
    ContentType: "text/csv",
  }

  try {
    const upload = new Upload({
      client: s3Client,
      params: uploadParams,
    })

    const result:any = await upload.done()
    return { success: true, url: result?.Location }
  } catch (error: any) {
    logger.error("S3 Upload Error (v3):", error)
    return { success: false, message: "Error uploading to S3: " + error.message }
  }
}

export const saveInvalidOrdersToCSV = async (invalidOrders: Record<string, string>[], filePath: string) => {
  try {
    const dir = path.dirname(filePath)
    await fs.promises.mkdir(dir, { recursive: true })
    const parser = new Parser()
    const csv = parser.parse(invalidOrders)

    await fs.promises.writeFile(filePath, csv)
    return { success: true, filePath }
  } catch (error) {
    logger.error("Failed to generate CSV:", error)
    return { success: false, message: "CSV generation failed" }
  }
}

export async function getCsvRowCount(filePath: fs.PathLike) {
  return new Promise((resolve, reject) => {
    let rowCount = 0
    const stream = fs.createReadStream(filePath)

    stream.on("data", (chunk) => {
      for (let i = 0; i < chunk.length; ++i) {
        if (chunk[i] === 10) rowCount++
      }

      if (rowCount > 10000 + 1) {
        stream.destroy()
        reject(new Error("CSV row limit exceeded (max 10,000 rows allowed)"))
      }
    })

    stream.on("end", () => resolve(rowCount - 1))
    stream.on("error", reject)
  })
}

export function getCsvLineCount(filePath: string): number {
  try {
    const resolvedPath = path.resolve(filePath)

    if (!fs.existsSync(resolvedPath)) {
      throw new Error(`File does not exist: ${resolvedPath}`)
    }

    if (fs.statSync(resolvedPath).isDirectory()) {
      throw new Error(`Expected a file, but received a directory: ${resolvedPath}`)
    }

    const output = execSync(`wc -l < "${resolvedPath}"`).toString().trim()
    const numLines = parseInt(output, 10)

    return numLines 
  } catch (err: any) {
    console.error("Error running wc -l:", err.message)
    return -1
  }
}

export const validateCSVHeadersStrict = (filePath: string): { success: boolean; message?: string } => {
  try {
    const content = fs.readFileSync(filePath, "utf8")

    const firstLine = content.split("\n")[0]
    console.log("First line:", firstLine)

    const normalizedHeaders = firstLine.split(",").map((h) => h.trim().toLowerCase().replace(/\s+/g, "_"))

    const missingFields = requiredFields.filter((field) => !normalizedHeaders.includes(field))
    const extraFields = normalizedHeaders.filter((field) => !requiredFields.includes(field))

    if (missingFields.length > 0) {
      return { success: false, message: `Missing required headers: ${missingFields.join(", ")}` }
    }

    if (extraFields.length > 0) {
      return { success: false, message: `Unexpected headers found: ${extraFields.join(", ")}` }
    }

    return { success: true }
  } catch (err) {
    return {
      success: false,
      message: err instanceof Error ? err.message : "Unknown error during validation",
    }
  }
}

export const checkForDuplicates = async (
  orderId: string,
  orderStatus: string,
  buyerAppId: string,
  recordMap: Map<string, { orderStatus: string; totalPrice: number }>,
  total_price: number,
): Promise<{ success: boolean; message?: string }> => {
  try {
    if (recordMap.has(orderId)) {
      const existing = recordMap.get(orderId)
      if ((existing?.orderStatus.toLowerCase() === orderStatus.toLowerCase()) && existing?.orderStatus !== "partially_cancelled") {
        return {
          success: false,
          message: `Duplicate order ${orderId} with status ${orderStatus} in the current batch`,
        }
      }

      if (orderStatus.toLowerCase() === "partially_cancelled" || orderStatus.toLowerCase() === "cancelled") {
        const activeOrder = recordMap.get(orderId)
        if (activeOrder && activeOrder.orderStatus === "active" && activeOrder.totalPrice < total_price) {
          return {
            success: false,
            message: `Order ${orderId} with status ${orderStatus} can't have GMV greater than active order`,
          }
        }
      }
    }

    if (orderStatus !== 'partially_cancelled') {
    const existingOrderInDb = await prisma.orderData.findFirst({
      where: {
        order_id: orderId,
        order_status: orderStatus,
        buyer_app_id: buyerAppId,
      }
    })

    if (existingOrderInDb) {
      return {
        success: false,
        message: `Order ${orderId} with status ${orderStatus} already exists in the database`,
      }
    }
  }

    if (orderStatus.toLowerCase() === "partially_cancelled" || orderStatus.toLowerCase() === "cancelled") {
      const activeOrder = await prisma.orderData.findFirst({
        where: {
          order_id: orderId,
          buyer_app_id: buyerAppId,
        },
        orderBy: {
          timestamp_created: "desc",
        },
        select: {
          total_price: true,
        },
      })

      if (activeOrder && activeOrder.total_price < total_price) {
        return {
          success: false,
          message: `Order ${orderId} with status ${orderStatus} can't have GMV greater than active order`,
        }
      }
    }

    return { success: true }
  } catch (error) {
    return {
      success: false,
      message: `Error checking for duplicates: ${error instanceof Error ? error.message : String(error)}`,
    }
  }
}

export const getErrorCode = (error: any): string => {
  const message = error?.meta?.message || error?.message
  if (message) {
    const errCodeIndex = message.indexOf("ERR_CODE:")
    if (errCodeIndex !== -1) {
      const extractedMessage = message.slice(errCodeIndex)
      let temp = `${extractedMessage}`
      temp = temp.split(":")[2].split(",")[0]
      return temp
    } else {
      logger.error("Error Message:", message)
      return 'Error inserting bulk data'
    }
  } else
  return 'Error inserting bulk data'
}