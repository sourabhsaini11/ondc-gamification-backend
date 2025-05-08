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
    "*/1 * * * *",
    async () => {
      try {
        logger.info(`*=== Running aggregatePointsSummary Cron Job ===*`)
        await aggregatePointsSummary()
      } catch (error) {
        logger.error(`*=== Error in aggregatePointsSummary Cron Job: ${error} ===*`)
      }
    },
    null,
    true,
    "Asia/Calcutta",
  ),
    new CronJob(
      "0 0 * * *",
      async () => {
        try {
          logger.info(`*=== Running Highest GMV AND ORDER ===*`)
          await highestGmvandhighestOrder()
        } catch (error) {
          logger.error(`*=== Error in Weekly Winner Cancellation Cron Job: ${error} ===*`)
        }
      },
      null,
      true,
      "Asia/Calcutta",
    ),
    new CronJob(
      "0 0 * * *",
      async () => {
        try {
          logger.info(`*=== Running Highest GMV AND ORDER ===*`)
          await getTodayFileContentsWithValidation()
        } catch (error) {
          logger.error(`*=== Error in Weekly Winner Cancellation Cron Job: ${error} ===*`)
        }
      },
      null,
      true,
      "Asia/Calcutta",
    ),
    new CronJob("0 8 * * *", DayWinnerUpdate, null, true, "Asia/Calcutta")

  new CronJob("0 8 * * 0", WeeklyWinnerUpdate, null, true, "Asia/Calcutta")

  new CronJob("0 8 1 * *", MonthlyWinnerUpdate, null, true, "Asia/Calcutta")
}
