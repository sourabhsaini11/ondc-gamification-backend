import { PrismaClient } from "@prisma/client"
// import moment from "moment"

const prisma = new PrismaClient()

export const aggregatePointsSummary = async () => {
  try {
    const aggregatedData: any = await prisma.$queryRaw`
            SELECT game_id, 
                   SUM(points) AS total_points, 
                   COUNT(order_id) AS total_orders, 
                   SUM(gmv) AS total_gmv
            FROM "orderData"
            GROUP BY game_id;
        `

    // Upsert aggregated data into leaderboard
    for (const { game_id, total_points, total_orders, total_gmv } of aggregatedData) {
      console.log("game_id, total_points, total_orders, total_gmv", game_id, total_points, total_orders, total_gmv)
      const data = {
        game_id,
        total_points: Number(total_points),
        total_orders: Number(total_orders),
        total_gmv: Number(total_gmv),
      }
      await prisma.leaderboard.upsert({
        where: { game_id },
        update: data,
        create: data,
      })
    }

    console.log(`Aggregated data successfully updated for ${aggregatedData.length} game IDs.`)

    await createOrRefreshLeaderboardView()
    await createOrRefreshWeeklyLeaderboardView()
    await createOrRefreshMonthlyLeaderboardView()

    return {
      statusCode: 200,
      body: `Updated ${aggregatedData.length} records in leaderboard`,
    }
  } catch (error) {
    console.error("Error aggregating points summary:", error)
    return { statusCode: 500, body: "Internal Server Error" }
  }
}

export const createOrRefreshLeaderboardView = async () => {
  try {
    const todayDate = new Date().toISOString().split("T")[0] // YYYY-MM-DD

    const orderCheck = await prisma.$queryRaw`
  SELECT COUNT(*) AS order_count
  FROM "orderData"
  WHERE timestamp_created >= ${todayDate}::DATE
  AND timestamp_created < (${todayDate}::DATE + INTERVAL '1 day');
`

    console.log("Orders found for", todayDate, ":", orderCheck)

    // Create or replace the daily leaderboard view
    await prisma.$executeRawUnsafe(`DROP VIEW IF EXISTS daily_top_leaderboard;`)

    // Create the new daily leaderboard view
    const previewResults = await prisma.$executeRawUnsafe(`
      CREATE VIEW daily_top_leaderboard AS
      WITH all_orders AS (
          -- Get all orders within the given date range
          SELECT 
              game_id,
              order_id,
              points,
              gmv,
              order_status
          FROM public."orderData"
          WHERE timestamp_created >= DATE_TRUNC('day', '${todayDate}'::TIMESTAMP)
                AND timestamp_created < DATE_TRUNC('day', '${todayDate}'::TIMESTAMP) + INTERVAL '1 day'
      ),
      cancelled_orders AS (
          -- Get order_ids that have at least one cancelled or partially_cancelled order
          SELECT DISTINCT order_id
          FROM all_orders
          WHERE order_status IN ('cancelled', 'partially_cancelled')
      ),
      valid_orders AS (
          -- Exclude orders that are in the cancelled_orders list
          SELECT 
              game_id,
              order_id
          FROM all_orders
          WHERE order_id NOT IN (SELECT order_id FROM cancelled_orders)
      )
      SELECT 
          a.game_id,
          SUM(a.points) AS total_points,  -- Summing all points (including cancelled orders)
          SUM(a.gmv) AS total_gmv,        -- Summing all GMV (including cancelled orders)
          COUNT(v.order_id) AS total_orders  -- Only counting valid orders (excluding cancelled)
      FROM all_orders a
      LEFT JOIN valid_orders v ON a.order_id = v.order_id
      GROUP BY a.game_id;
      
          `)
    //     const previewResults = await prisma.$executeRawUnsafe(`
    //  CREATE OR REPLACE VIEW daily_top_leaderboard AS
    // WITH first_status AS (
    //     SELECT
    //         game_id,
    //         order_id,
    //         points,
    //         gmv,
    //         order_status,
    //         timestamp_created,
    //         -- Get the first status for each order_id within the day
    //         ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY timestamp_created ASC) AS rn
    //     FROM public."orderData"
    //     WHERE timestamp_created >= CURRENT_DATE
    //       AND timestamp_created < CURRENT_DATE + INTERVAL '1 day'
    // ),
    // valid_orders AS (
    //     SELECT
    //         game_id,
    //         order_id,
    //         points,
    //         gmv,
    //         order_status,
    //         -- Only include orders where the first status is 'created'
    //         CASE WHEN rn = 1 AND order_status = 'created' THEN 1 ELSE 0 END AS valid_order
    //     FROM first_status
    // )
    // SELECT
    //     game_id,
    //     SUM(points)::DOUBLE PRECISION AS total_points,
    //     COUNT(valid_order) AS total_orders,  -- Count only valid orders
    //     SUM(gmv)::BIGINT AS total_gmv,
    //     CURRENT_DATE AS leaderboard_day_start
    // FROM valid_orders
    // WHERE valid_order = 1  -- Only count valid orders
    // GROUP BY game_id
    // HAVING SUM(points) >= 0  -- Exclude users with negative points
    // ORDER BY total_points DESC;
    //     `)

    console.log(`Leaderboard view updated for ${todayDate} with cancellation handling, ${previewResults}`)
    return {
      statusCode: 200,
      body: `Leaderboard view updated for ${todayDate}, ${previewResults}`,
    }
  } catch (error) {
    console.error("Error creating/updating leaderboard view:", error)
    return {
      statusCode: 500,
      body: "Internal Server Error",
    }
  }
}

export const createOrRefreshWeeklyLeaderboardView = async () => {
  try {
    // Get today's date and determine the start of the current week (Monday)
    const todayDate = new Date()
    const currentWeekStart = new Date(todayDate)
    currentWeekStart.setDate(todayDate.getDate() - todayDate.getDay() + 1) // Monday of this week
    const currentWeekStartStr = currentWeekStart.toISOString().split("T")[0] // YYYY-MM-DD

    console.log(`Updating weekly leaderboard view for the week starting: ${currentWeekStartStr}`)

    // Check if the leaderboard view exists
    const viewCheck: any = await prisma.$queryRaw`
        SELECT EXISTS (
          SELECT 1 FROM information_schema.views 
          WHERE table_name = 'weekly_top_leaderboard'
        ) AS view_exists;
      `

    const viewExists = viewCheck[0]?.view_exists
    console.log("viewExists", viewExists)

    if (viewExists) {
      // Get the last week's start date from the existing view
      const lastWeekCheck: any = await prisma.$queryRaw`
          SELECT DISTINCT leaderboard_week_start FROM weekly_top_leaderboard LIMIT 1;
        `

      const lastWeekStart = lastWeekCheck.length > 0 ? lastWeekCheck[0].leaderboard_week_start : null

      if (lastWeekStart && lastWeekStart.toISOString().split("T")[0] !== currentWeekStartStr) {
        console.log(`Week changed from ${lastWeekStart} to ${currentWeekStartStr}. Resetting weekly leaderboard view.`)
        await prisma.$executeRawUnsafe(`DROP VIEW IF EXISTS weekly_top_leaderboard;`)
      }
    }

    // Create or refresh the weekly leaderboard view
    const previewResults = await prisma.$executeRawUnsafe(`
     CREATE OR REPLACE VIEW weekly_top_leaderboard AS
WITH first_status AS (
    SELECT 
        game_id,
        order_id,
        points,
        gmv,
        order_status,
        timestamp_created,
        -- Get the first status for each order_id within the week
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY timestamp_created ASC) AS rn
    FROM public."orderData"
    WHERE timestamp_created >= DATE_TRUNC('week', CURRENT_DATE)
      AND timestamp_created < DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '7 days'
),
valid_orders AS (
    SELECT
        game_id,
        order_id,
        points,
        gmv,
        order_status,
        -- Only include orders where the first status is 'created'
        CASE WHEN rn = 1 AND order_status = 'created' THEN 1 ELSE 0 END AS valid_order
    FROM first_status
)
SELECT 
    game_id,
    SUM(points)::DOUBLE PRECISION AS total_points,
    COUNT(valid_order) AS total_orders,  -- Count only valid orders
    SUM(gmv)::BIGINT AS total_gmv,
    DATE_TRUNC('week', CURRENT_DATE)::DATE AS leaderboard_week_start
FROM valid_orders
WHERE valid_order = 1  -- Only count valid orders
GROUP BY game_id
HAVING SUM(points) >= 0  -- Exclude users with negative points
ORDER BY total_points DESC;
    `)

    console.log(`Weekly leaderboard view updated for the week starting ${currentWeekStartStr}., ${previewResults}`)
    return {
      statusCode: 200,
      body: `Weekly leaderboard view created/updated for the week starting ${currentWeekStartStr}, ${previewResults}.`,
    }
  } catch (error) {
    console.error("Error creating/updating weekly leaderboard view:", error)
    return {
      statusCode: 500,
      body: "Internal Server Error",
    }
  }
}

export const createOrRefreshMonthlyLeaderboardView = async () => {
  try {
    // Get today's date
    const todayDate = new Date()

    // Calculate the start of the current month (1st day of this month)
    const currentMonthStart = new Date(todayDate.getFullYear(), todayDate.getMonth(), 1)
    const currentMonthStartStr = currentMonthStart.toISOString().split("T")[0] // YYYY-MM-DD

    console.log(`Updating monthly leaderboard view for the month starting: ${currentMonthStartStr}`)

    // Check if the leaderboard view exists
    const viewCheck: any = await prisma.$queryRaw`
          SELECT EXISTS (
            SELECT 1 FROM information_schema.views 
            WHERE table_name = 'monthly_top_leaderboard'
          ) AS view_exists;
        `

    const viewExists = viewCheck[0]?.view_exists
    console.log("viewExists:", viewExists)

    if (viewExists) {
      // Check the last recorded month in the existing leaderboard view
      const lastMonthCheck: any = await prisma.$queryRaw`
            SELECT DISTINCT leaderboard_month_start FROM monthly_top_leaderboard LIMIT 1;
          `

      const lastMonthStart = lastMonthCheck.length > 0 ? lastMonthCheck[0].leaderboard_month_start : null

      if (lastMonthStart && lastMonthStart.toISOString().split("T")[0] !== currentMonthStartStr) {
        console.log(
          `Month changed from ${lastMonthStart} to ${currentMonthStartStr}. Resetting monthly leaderboard view.`,
        )
        await prisma.$executeRawUnsafe(
          `ALTER VIEW monthly_top_leaderboard RENAME COLUMN leaderboard_month_start TO month;`,
        )
      }
    }

    // Create or refresh the monthly leaderboard view
    await prisma.$executeRawUnsafe(`DROP VIEW IF EXISTS monthly_top_leaderboard`)
    const previewResults = await prisma.$executeRawUnsafe(`
      CREATE OR REPLACE VIEW monthly_top_leaderboard AS
      WITH all_orders AS (
          -- Get all orders from the given month
          SELECT 
              game_id,
              order_id,
              points,
              gmv,
              order_status
          FROM public."orderData"
          WHERE DATE(timestamp_created) >= '2025-02-01' -- Adjust as needed
      ),
      cancelled_orders AS (
          -- Get order_ids that have at least one cancelled order
          SELECT DISTINCT order_id
          FROM all_orders 
          WHERE order_status = 'cancelled'
      ),
      valid_orders AS (
          -- Exclude fully cancelled orders
          SELECT 
              game_id,
              order_id
          FROM all_orders
          WHERE order_id NOT IN (SELECT order_id FROM cancelled_orders)
      )
      SELECT 
          a.game_id,
          COUNT(DISTINCT v.order_id) AS total_orders,  -- Count only valid orders
          SUM(a.points) AS total_points,  -- Sum all points
          SUM(a.gmv) AS total_gmv  -- Sum all GMV
      FROM all_orders a
      LEFT JOIN valid_orders v ON a.order_id = v.order_id
      GROUP BY a.game_id;
    `)

    //   const previewResults = await prisma.$executeRawUnsafe(`
    //     CREATE OR REPLACE VIEW monthly_top_leaderboard AS
    // WITH first_status AS (
    //     SELECT
    //         game_id,
    //         order_id,
    //         points,
    //         gmv,
    //         order_status,
    //         timestamp_created,
    //         ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY timestamp_created ASC) AS rn
    //     FROM public."orderData"
    //     WHERE DATE(timestamp_created) >= '${currentMonthStart.toISOString().split("T")[0]}'::DATE
    // ),
    // valid_orders AS (
    //     SELECT
    //         game_id,
    //         order_id,
    //         points,
    //         gmv,
    //         order_status,
    //         CASE WHEN rn = 1 AND order_status = 'created' THEN 1 ELSE 0 END AS valid_order
    //     FROM first_status
    // ),
    // unique_order_counts AS (
    //     -- Select only one unique occurrence of (order_id, game_id)
    //     SELECT DISTINCT ON (order_id, game_id) game_id, order_id FROM valid_orders WHERE valid_order = 1
    // )
    // SELECT
    //     v.game_id,
    //     SUM(v.points)::DOUBLE PRECISION AS total_points,  -- Sum all points (including duplicates)
    //     COUNT(DISTINCT u.order_id) AS total_orders,  -- Ensure unique (order_id, game_id) count
    //     SUM(v.gmv)::BIGINT AS total_gmv,  -- Sum all GMV
    //     '${currentMonthStart.toISOString().split("T")[0]}'::DATE AS leaderboard_month_start
    // FROM valid_orders v
    // LEFT JOIN unique_order_counts u ON v.order_id = u.order_id AND v.game_id = u.game_id  -- Unique order counting
    // GROUP BY v.game_id
    // HAVING SUM(v.points) >= 0  -- Exclude users with negative points
    // ORDER BY total_points DESC;
    // `)

    //     const previewResults = await prisma.$executeRawUnsafe(`
    //         CREATE OR REPLACE VIEW monthly_top_leaderboard AS
    // WITH first_status AS (
    //     SELECT
    //         game_id,
    //         order_id,
    //         points,
    //         gmv,
    //         order_status,
    //         timestamp_created,
    //         -- Rank orders by timestamp_created, ensuring we get the first status for each order_id
    //         ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY timestamp_created ASC) AS rn
    //     FROM public."orderData"
    //     WHERE DATE(timestamp_created) >= '${currentMonthStart.toISOString().split("T")[0]}'::DATE
    // ),
    // valid_orders AS (
    //     SELECT
    //         game_id,
    //         order_id,
    //         points,
    //         gmv,
    //         order_status,
    //         -- Only include orders where the first status is 'created'
    //         CASE WHEN rn = 1 AND order_status = 'created' THEN 1 ELSE 0 END AS valid_order
    //     FROM first_status
    // )
    // SELECT
    //     game_id,
    //     SUM(points)::DOUBLE PRECISION AS total_points,
    //     COUNT(valid_order) AS total_orders,  -- Count only the valid orders
    //     SUM(gmv)::BIGINT AS total_gmv,
    //     '${currentMonthStart.toISOString().split("T")[0]}'::DATE AS leaderboard_month_start
    // FROM valid_orders
    // WHERE valid_order = 1  -- Only count valid orders
    // GROUP BY game_id
    // HAVING SUM(points) >= 0  -- Exclude users with negative points
    // ORDER BY total_points DESC;

    //     `)

    console.log(`Monthly leaderboard view updated for the month starting ${currentMonthStartStr}, ${previewResults}`)
    return {
      statusCode: 200,
      body: `Monthly leaderboard view created/updated for the month starting ${currentMonthStartStr}, ${previewResults}.`,
    }
  } catch (error) {
    console.error("Error creating/updating monthly leaderboard view:", error)
    return {
      statusCode: 500,
      body: "Internal Server Error",
    }
  }
}

export const getDailyLeaderboardData = async () => {
  try {
    const leaderboardData: any = await prisma.$queryRaw`
            SELECT * FROM daily_top_leaderboard 
            ORDER BY total_points DESC;
          `

    // Convert BigInt values to strings to avoid serialization issues
    const updatedData = leaderboardData.map((row: any) => ({
      ...row,
      total_orders: row.total_orders.toString(),
      total_gmv: row.total_gmv.toString(),
    }))

    return {
      statusCode: 200,
      body: updatedData,
    }
  } catch (error) {
    console.error("Error fetching daily leaderboard data:", error)
    return {
      statusCode: 500,
      body: "Internal Server Error",
    }
  }
}

export const getWeeklyLeaderboardData = async () => {
  try {
    console.log("Fetching weekly leaderboard data...")

    const leaderboardData: any = await prisma.$queryRaw`
          SELECT * FROM weekly_top_leaderboard
          ORDER BY total_points DESC;
        `

    const updatedData = leaderboardData.map((row: any) => ({
      ...row,
      total_points: row.total_points.toString(),
      total_orders: row.total_orders.toString(),
      total_gmv: row.total_gmv.toString(),
    }))

    console.log("Weekly leaderboard data retrieved successfully.", updatedData)

    return {
      statusCode: 200,
      body: updatedData,
    }
  } catch (error) {
    console.error("Error fetching weekly leaderboard data:", error)
    return {
      statusCode: 500,
      body: "Internal Server Error",
    }
  }
}

export const getMonthlyLeaderboardData = async () => {
  try {
    const leaderboardData: any = await prisma.$queryRaw`
          SELECT * FROM monthly_top_leaderboard 
          ORDER BY total_points DESC;
        `

    const updatedData = leaderboardData.map((row: any) => ({
      ...row,
      total_orders: row.total_orders.toString(),
      total_points: row.total_points.toString(),
      total_gmv: row.total_gmv.toString(),
    }))

    return {
      statusCode: 200,
      body: updatedData,
    }
  } catch (error) {
    console.error("Error fetching monthly leaderboard data:", error)
    return {
      statusCode: 500,
      body: "Internal Server Error",
    }
  }
}

export const fetchLeaderboardData = async () => {
  try {
    const leaderboardData = await prisma.$queryRaw`
          SELECT game_id, total_points, total_orders, total_gmv
          FROM daily_top_leaderboard
          ORDER BY total_points DESC;
        `

    return {
      statusCode: 200,
      body: leaderboardData,
    }
  } catch (error) {
    console.error("Error fetching leaderboard data:", error)
    return {
      statusCode: 500,
      body: "Internal Server Error",
    }
  }
}

export const leaderboardTrigger = async () => {
  try {
    console.log("🔄 Setting up leaderboard trigger...")

    // Create or replace the leaderboard update function
    let res = await prisma.$executeRawUnsafe(`
          CREATE OR REPLACE FUNCTION update_leaderboard()
          RETURNS TRIGGER AS $$
          BEGIN
              INSERT INTO leaderboard (game_id, total_points, total_orders, total_gmv)
              VALUES (
                  NEW.game_id, 
                  (SELECT SUM(points) FROM "orderData" WHERE game_id = NEW.game_id),
                  (SELECT COUNT(order_id) FROM "orderData" WHERE game_id = NEW.game_id),
                  (SELECT SUM(gmv) FROM "orderData" WHERE game_id = NEW.game_id)
              )
              ON CONFLICT (game_id) 
              DO UPDATE SET 
                  total_points = EXCLUDED.total_points,
                  total_orders = EXCLUDED.total_orders,
                  total_gmv = EXCLUDED.total_gmv;
    
              RETURN NEW;
          END;
          $$ LANGUAGE plpgsql;
        `)

    console.log("✅ Leaderboard update function created.", res)

    // Remove existing trigger if it exists
    await prisma.$executeRawUnsafe(`
        DROP TRIGGER IF EXISTS trigger_update_leaderboard ON "orderData";
      `)

    console.log("🔄 Old leaderboard trigger removed (if it existed).")

    // Create the new trigger
    await prisma.$executeRawUnsafe(`
        CREATE TRIGGER trigger_update_leaderboard
        AFTER INSERT OR UPDATE OR DELETE ON "orderData"
        FOR EACH ROW
        EXECUTE FUNCTION update_leaderboard();
      `)

    console.log("✅ New leaderboard trigger created successfully.")
  } catch (error) {
    console.error("❌ Error setting up leaderboard trigger:", error)
  }
}

export const checkDailyWinnerCancellation = async () => {
  try {
    // Get the previous day (Day 1)
    const previousDay = new Date()
    previousDay.setDate(previousDay.getDate() - 1)
    previousDay.setHours(0, 0, 0, 0) // Start of Day 1 (previous day)

    const previousDayEnd = new Date(previousDay)
    previousDayEnd.setHours(23, 59, 59, 999) // End of Day 1

    // Get today's date (Day 2)
    const today = new Date()
    today.setHours(0, 0, 0, 0) // Start of Day 2 (today)

    const todayEnd = new Date(today)
    todayEnd.setHours(23, 59, 59, 999) // End of Day 2 (today)

    // 1. Find the winner for the previous day (day_1)
    const dailyWinner = await prisma.orderData.groupBy({
      by: ["uid"],
      _sum: { points: true },
      where: {
        timestamp_created: {
          gte: previousDay,
          lte: previousDayEnd,
        },
        highest_gmv_for_day: true, // Ensure it's a winner
      },
      orderBy: {
        _sum: { points: "desc" },
      },
      take: 2, // Only get the top winner
    })

    if (!dailyWinner.length) {
      console.log("No winner found for the previous day.")
      return
    }

    const winnerUid = dailyWinner[0].uid

    // 2. Check if the winner canceled any orders on the current day (day_2)
    const canceledOrders = await prisma.orderData.findMany({
      where: {
        uid: winnerUid,
        timestamp_created: {
          gte: today,
          lte: todayEnd,
        },
        order_status: "cancelled",
      },
    })

    if (canceledOrders.length > 0 && dailyWinner.length > 0) {
      const firstWinner = dailyWinner[0]
      const secondWinner = dailyWinner[1]

      // Check if _sum and points are defined for both firstWinner and secondWinner
      if (firstWinner?._sum?.points != null && secondWinner?._sum?.points != null) {
        if (firstWinner._sum.points < secondWinner._sum.points) {
          // await handleOrderCancellationAndViolation(winnerUid, "daily", 1)
          await prisma.orderData.updateMany({
            where: {
              uid: winnerUid,
              timestamp_created: {
                gte: previousDay,
                lte: previousDayEnd,
              },
            },
            data: {
              highest_gmv_for_day: false,
              highest_orders_for_day: false,
            },
          })
        } else {
          console.log("winner position has not been affected")
        }

        console.log(`Winner ${winnerUid} canceled orders today. Winner status removed for the previous day.`)
      } else {
        console.log("One of the winners has missing points data.")
      }
    } else {
      console.log(`Winner ${winnerUid} did not cancel orders today. Status remains for the previous day.`)
    }
  } catch (error) {
    console.error("Error checking daily winner cancellations:", error)
  }
}

// export const handleOrderCancellationAndViolation = async (uid: string, violationType: string, weekNumber: number) => {
//   try {
//     // 1. Check if the user already has violations tracked in the ViolationTracker table
//     const violationTracker = await prisma.violationTracker.findFirst({
//       where: {
//         uid,
//         violation_type: violationType,
//         week_number: weekNumber,
//       },
//     })

//     // 2. If no violation record found, create a new one
//     if (!violationTracker) {
//       await prisma.violationTracker.create({
//         data: {
//           uid,
//           violation_type: violationType,
//           week_number: weekNumber,
//           violation_count: 1, // First violation
//         },
//       })

//       // Deduct 200 points for the first violation
//       await prisma.orderData.updateMany({
//         where: { uid },
//         data: {
//           points: {
//             decrement: 200,
//           },
//         },
//       })

//       console.log(`User ${uid} has their first violation. Deducted 200 points.`)
//     } else {
//       // 3. Update violation count if violation record exists
//       const updatedViolationCount = violationTracker.violation_count + 1

//       if (updatedViolationCount === 2) {
//         // Second violation: set points to 0
//         await prisma.orderData.updateMany({
//           where: { uid },
//           data: {
//             points: 0,
//           },
//         })

//         console.log(`User ${uid} has their second violation. Points set to 0.`)
//       } else if (updatedViolationCount >= 3) {
//         // Third violation: blacklist the user
//         await prisma.violationTracker.update({
//           where: {
//             id: violationTracker.id,
//           },
//           data: {
//             is_blacklisted: true,
//           },
//         })

//         console.log(`User ${uid} has been blacklisted due to third violation.`)
//       }

//       // Update the violation count
//       await prisma.violationTracker.update({
//         where: {
//           id: violationTracker.id,
//         },
//         data: {
//           violation_count: updatedViolationCount,
//         },
//       })
//     }
//   } catch (error) {
//     console.error("Error handling order cancellation and violation:", error)
//   }
// }

export const checkWeeklyWinnerCancellation = async () => {
  try {
    // Get the previous week (Week 1)
    const startOfWeek1 = new Date()
    startOfWeek1.setDate(startOfWeek1.getDate() - (startOfWeek1.getDay() + 7)) // Start of previous week (Week 1)
    startOfWeek1.setHours(0, 0, 0, 0)

    const endOfWeek1 = new Date(startOfWeek1)
    endOfWeek1.setDate(startOfWeek1.getDate() + 6) // End of previous week (Week 1)
    endOfWeek1.setHours(23, 59, 59, 999)

    // Get this week (Week 2)
    const startOfWeek2 = new Date()
    startOfWeek2.setDate(startOfWeek2.getDate() - startOfWeek2.getDay()) // Start of this week (Week 2)
    startOfWeek2.setHours(0, 0, 0, 0)

    const endOfWeek2 = new Date(startOfWeek2)
    endOfWeek2.setDate(startOfWeek2.getDate() + 6) // End of this week (Week 2)
    endOfWeek2.setHours(23, 59, 59, 999)

    // 1. Find the winner for the previous week (week_1)
    const weeklyWinner = await prisma.orderData.groupBy({
      by: ["uid"],
      _sum: { points: true },
      where: {
        timestamp_created: {
          gte: startOfWeek1,
          lte: endOfWeek1,
        },
        highest_gmv_for_day: true, // Ensure it's a winner
      },
      orderBy: {
        _sum: { points: "desc" },
      },
      take: 1, // Only get the top winner
    })

    if (!weeklyWinner.length) {
      console.log("No winner found for the previous week.")
      return
    }

    const winnerUid = weeklyWinner[0].uid

    // 2. Check if the winner canceled any orders in Week 2 (this week)
    const canceledOrdersInWeek2 = await prisma.orderData.findMany({
      where: {
        uid: winnerUid,
        timestamp_created: {
          gte: startOfWeek2,
          lte: endOfWeek2,
        },
        order_status: "cancelled",
      },
    })

    if (canceledOrdersInWeek2.length > 0) {
      // 3. If they have canceled orders in Week 2, remove their winner status for Week 1
      await prisma.orderData.updateMany({
        where: {
          uid: winnerUid,
          timestamp_created: {
            gte: startOfWeek1,
            lte: endOfWeek1,
          },
        },
        data: {
          highest_gmv_for_day: false,
          highest_orders_for_day: false,
        },
      })

      console.log(`Winner ${winnerUid} canceled orders in Week 2. Winner status removed for Week 1.`)

      // 4. Call handleOrderCancellationAndViolation to track violation and adjust points/status
      // await handleOrderCancellationAndViolation(winnerUid, "weekly", 1)
    } else {
      console.log(`Winner ${winnerUid} did not cancel orders in Week 2. Status remains for Week 1.`)
    }
  } catch (error) {
    console.error("Error checking weekly winner cancellations:", error)
  }
}
