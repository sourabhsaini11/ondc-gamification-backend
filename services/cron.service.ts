import { CronJob } from "cron"
import { aggregatePointsSummary } from "./points.servce"

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
  )
}
