import { CronJob } from "cron"
import {
  aggregatePointsSummary,
  // , checkWeeklyWinnerCancellation, checkDailyWinnerCancellation
} from "./points.servce"
// import { aggregateDailyGmvAndPoints } from "./index"

export const aggregatePointsCron = () => {
  new CronJob(
    // "*/1 * * *", // Runs every 10 minutes
    "*/1 * * * *", // Runs every 10 minutes
    async () => {
      try {
        console.log(`*=== Running aggregatePointsSummary Cron Job ===*`)
        await aggregatePointsSummary()
        console.log(`*=== aggregatePointsSummary Job Completed Successfully ===*`)
      } catch (error) {
        console.error(`*=== Error in aggregatePointsSummary Cron Job: ${error} ===*`)
      }
    },
    null,
    true,
    "Asia/Calcutta",
  ),
    // new CronJob(
    //   // "*/1 * * *", // Runs every 10 minutes
    //   // "*/1 * * * *", // Runs every 10 minutes
    //   "*/10 * * * * *", // Runs every 30 seconds
    //   async () => {
    //     try {
    //       console.log(`*=== Running aggregateDailyGmvAndPoints Cron Job ===*`)
    //       await aggregateDailyGmvAndPoints()
    //       console.log(`*=== aggregateDailyGmvAndPoints Job Completed Successfully ===*`)
    //     } catch (error) {
    //       console.error(`*=== Error in aggregateDailyGmvAndPoints Cron Job: ${error} ===*`)
    //     }
    //   },
    //   null,
    //   true,
    //   "Asia/Calcutta",
    // ),
    new CronJob(
      "0 8 * * 1", // Runs every Monday at 8 AM
      async () => {
        try {
          console.log(`*=== Running Weekly Winner Cancellation Cron Job ===*`)
          // await checkWeeklyWinnerCancellation()
          console.log(`*=== Weekly Winner Cancellation Check Completed ===*`)
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
      async () => {
        try {
          console.log(`*=== Running Daily Winner Cancellation Cron Job ===*`)
          // await checkDailyWinnerCancellation()
          console.log(`*=== Daily Winner Cancellation Check Completed ===*`)
        } catch (error) {
          console.error(`*=== Error in Daily Winner Cancellation Cron Job: ${error} ===*`)
        }
      },
      null,
      true,
      "Asia/Calcutta", // Adjust to your time zone
    )
}
