import { CronJob } from "cron"
import {
  aggregatePointsSummary,
  DayWinnerUpdate,
  WeeklyWinnerUpdate,
  MonthlyWinnerUpdate,
  highestGmvandhighestOrder,
} from "./points.servce"
import { logger } from "../shared/logger"
import { getTodayFileContentsWithValidation } from "./index"

export const aggregatePointsCron = () => {
  new CronJob(
    // "*/1 * * *", // Runs every 10 minutes
    "*/1 * * * *", // Runs every 10 minutes
    async () => {
      try {
        logger.info(`*=== Running aggregatePointsSummary Cron Job ===*`)
        await aggregatePointsSummary()
      } catch (error) {
        console.error(`*=== Error in aggregatePointsSummary Cron Job: ${error} ===*`)
      }
    },
    null,
    true,
    "Asia/Calcutta",
  ),
    new CronJob(
      "0 0 * * *", // Runs every Monday at 8 AM
      async () => {
        try {
          logger.info(`*=== Running Highest GMV AND ORDER ===*`)
          await highestGmvandhighestOrder()
        } catch (error) {
          console.error(`*=== Error in Weekly Winner Cancellation Cron Job: ${error} ===*`)
        }
      },
      null,
      true,
      "Asia/Calcutta", // Adjust to your time zone
    ),
    new CronJob(
      "0 0 * * *", // Runs every Monday at 8 AM
      async () => {
        try {
          logger.info(`*=== Running Highest GMV AND ORDER ===*`)
          await getTodayFileContentsWithValidation()
        } catch (error) {
          console.error(`*=== Error in Weekly Winner Cancellation Cron Job: ${error} ===*`)
        }
      },
      null,
      true,
      "Asia/Calcutta", // Adjust to your time zone
    ),
    new CronJob(
      "0 8 * * *", // Runs every day at 8 AM
      DayWinnerUpdate,
      null,
      true,
      "Asia/Calcutta",
    )

  new CronJob(
    "0 8 * * 0", // Runs every Sunday at 8 AM (end of the week)
    WeeklyWinnerUpdate,
    null,
    true,
    "Asia/Calcutta",
  )

  new CronJob(
    // "*/1 * * * *", // Runs every 10 minutes
    "0 8 1 * *", // Runs on the first day of each month at 8 AM (storing last month's winners)
    MonthlyWinnerUpdate,
    null,
    true,
    "Asia/Calcutta",
  )
}
