import { Leaderboard, PrismaClient } from "@prisma/client"
import { insertrewardledgertesting } from "./index"
const prisma = new PrismaClient()

type aggregatedData = {
  game_id: string
  total_points: string
  total_orders: string
  total_gmv: string
}

export const aggregatePointsSummary = async () => {
  try {
    const aggregatedData: aggregatedData[] = await prisma.$queryRaw`
            SELECT game_id, 
                   SUM(points) AS total_points, 
                   COUNT(order_id) AS total_orders, 
                   SUM(gmv) AS total_gmv
            FROM "orderData"
            GROUP BY game_id;
        `

    // Upsert aggregated data into leaderboard
    for (const { game_id, total_points, total_orders, total_gmv } of aggregatedData) {
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

    //     const orderCheck = await prisma.$queryRaw`
    //   SELECT COUNT(*) AS order_count
    //   FROM "orderData"
    //   WHERE timestamp_created >= ${todayDate}::DATE
    //   AND timestamp_created < (${todayDate}::DATE + INTERVAL '1 day');
    // `

    // Create or replace the daily leaderboard view
    await prisma.$executeRawUnsafe(`DROP VIEW IF EXISTS daily_top_leaderboard;`)

    const previewResults = await prisma.$executeRawUnsafe(`
            CREATE VIEW daily_top_leaderboard AS
            WITH valid_orders AS (
    SELECT order_id ,buyer_app_id ,buyer_name
    FROM public."orderData"
    GROUP BY order_id ,buyer_app_id,buyer_name
    HAVING BOOL_AND(order_status <> 'cancelled')  -- Exclude orders where any entry is 'cancelled'
)
SELECT
    r.game_id,
    COALESCE(SUM(r.points), 0) AS total_points,
    COUNT(DISTINCT r.order_id)::BIGINT AS total_orders,  -- Count distinct order_ids
    COALESCE(SUM(r.gmv), 0)::BIGINT AS total_gmv,
    vo.buyer_app_id AS buyer_app_id,
    vo.buyer_name AS buyer_name,
    '${todayDate}'::DATE AS leaderboard_day_start
FROM public."rewardledgertesting" r
JOIN valid_orders vo ON vo.order_id = r.order_id
WHERE r.order_id IN (SELECT order_id FROM valid_orders)  -- Only include non-cancelled order_ids
GROUP BY r.game_id ,vo.buyer_app_id,vo.buyer_name
ORDER BY total_points DESC;
          `)

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
    currentWeekStart.setDate(todayDate.getDate() - todayDate.getDay() + (todayDate.getDay() === 0 ? -6 : 1)) // Monday of this week
    const currentWeekStartStr = currentWeekStart.toISOString().split("T")[0] // YYYY-MM-DD

    // Check if the leaderboard view exists
    const viewCheck: any = await prisma.$queryRaw`
        SELECT EXISTS (
          SELECT 1 FROM information_schema.views 
          WHERE table_name = 'weekly_top_leaderboard'
        ) AS view_exists;
      `

    const viewExists = viewCheck[0]?.view_exists

    if (viewExists) {
      // Get the last week's start date from the existing view
      const lastWeekCheck: any = await prisma.$queryRaw`
          SELECT DISTINCT leaderboard_week_start FROM weekly_top_leaderboard LIMIT 1;
        `

      const lastWeekStart = lastWeekCheck.length > 0 ? lastWeekCheck[0].leaderboard_week_start : null

      if (lastWeekStart && lastWeekStart.toISOString().split("T")[0] !== currentWeekStartStr) {
        console.log(`Week changed from ${lastWeekStart} to ${currentWeekStartStr}. Resetting weekly leaderboard view.`)
      }
    }

    await prisma.$executeRawUnsafe(`DROP VIEW IF EXISTS weekly_top_leaderboard;`)

    const previewResults = await prisma.$executeRawUnsafe(`
      CREATE VIEW weekly_top_leaderboard AS
      WITH valid_orders AS (
          SELECT order_id ,buyer_app_id ,buyer_name
          FROM public."orderData"
          GROUP BY order_id ,buyer_app_id ,buyer_name
          HAVING BOOL_AND(order_status <> 'cancelled')  -- Exclude orders where any entry is 'cancelled'
      )
      SELECT
          r.game_id,
          COALESCE(SUM(r.points), 0) AS total_points,
          COUNT(DISTINCT r.order_id)::BIGINT AS total_orders,  -- Only count valid order_ids
          COALESCE(SUM(r.gmv), 0)::BIGINT AS total_gmv,
          vo.buyer_app_id AS buyer_app_id ,
          vo.buyer_name AS buyer_name ,
          '${currentWeekStartStr}'::DATE AS leaderboard_week_start
      FROM public."rewardledgertesting" r 
      JOIN valid_orders vo ON vo.order_id = r.order_id
      WHERE DATE(r.created_at) >= '${currentWeekStartStr}'::DATE
        AND r.order_id IN (SELECT order_id FROM valid_orders)  -- Only include non-cancelled order_ids
      GROUP BY r.game_id ,vo.buyer_app_id,vo.buyer_name
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

    // Explicitly calculate the start of the current month (1st day of this month) in UTC
    const currentMonthStart = new Date(Date.UTC(todayDate.getUTCFullYear(), todayDate.getUTCMonth(), 1))

    // Convert the calculated date to a string format YYYY-MM-DD
    const currentMonthStartStr = currentMonthStart.toISOString().split("T")[0] // YYYY-MM-DD

    // Check if the leaderboard view exists
    const viewCheck: any = await prisma.$queryRaw`
      SELECT EXISTS (
        SELECT 1 FROM information_schema.views 
        WHERE table_name = 'monthly_top_leaderboard'
      ) AS view_exists;
    `

    const viewExists = viewCheck[0]?.view_exists

    if (viewExists) {
      // Check the last recorded month in the existing leaderboard view
      const lastMonthCheck: any = await prisma.$queryRaw`
            SELECT DISTINCT leaderboard_month_start FROM monthly_top_leaderboard LIMIT 1;
          `
      const lastMonthStart = lastMonthCheck.length > 0 ? lastMonthCheck[0].leaderboard_month_start : null
      if (lastMonthStart && lastMonthStart.toISOString().split("T")[0] !== currentMonthStartStr) {
        await prisma.$executeRawUnsafe(`ALTER VIEW monthly_top_leaderboard`)
      }
    }

    // Drop the view if it exists and recreate it with updated logic
    await prisma.$executeRawUnsafe(`DROP VIEW IF EXISTS monthly_top_leaderboard`)

    const previewResults = await prisma.$executeRawUnsafe(`
      CREATE OR REPLACE VIEW monthly_top_leaderboard AS
      WITH valid_orders AS (
          SELECT order_id ,buyer_app_id,buyer_name
          FROM public."orderData"
          GROUP BY order_id ,buyer_app_id,buyer_name
          HAVING BOOL_AND(order_status <> 'cancelled')  -- Exclude orders where any entry is 'cancelled'
      )
      SELECT
          r.game_id,
          COALESCE(SUM(r.points), 0) AS total_points,
          COUNT(DISTINCT r.order_id)::BIGINT AS total_orders,  -- Only count valid order_ids
          COALESCE(SUM(r.gmv), 0)::BIGINT AS total_gmv,
          vo.buyer_app_id AS buyer_app_id ,
          vo.buyer_name AS buyer_name,
          '${currentMonthStart.toISOString().split("T")[0]}'::DATE AS leaderboard_month_start
      FROM public."rewardledgertesting" r 
      JOIN valid_orders vo ON vo.order_id = r.order_id
      WHERE DATE(r.created_at) >= '${currentMonthStart.toISOString().split("T")[0]}'::DATE
        AND r.order_id IN (SELECT order_id FROM valid_orders)  -- Only include non-cancelled order_ids
      GROUP BY r.game_id , vo.buyer_app_id,vo.buyer_name
      ORDER BY total_points DESC;
    `)

    return {
      statusCode: 200,
      body: `Monthly leaderboard view created/updated for the month starting ${currentMonthStartStr}.`,
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
    const leaderboardData: Leaderboard[] = await prisma.$queryRaw`
            SELECT * FROM daily_top_leaderboard 
            ORDER BY total_points DESC;
          `

    const updatedData = leaderboardData.map((row: Leaderboard) => ({
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
    const leaderboardData: Leaderboard[] = await prisma.$queryRaw`
          SELECT * FROM weekly_top_leaderboard
          ORDER BY total_points DESC;
        `

    const updatedData = leaderboardData.map((row: Leaderboard) => ({
      ...row,
      total_points: row.total_points.toString(),
      total_orders: row.total_orders.toString(),
      total_gmv: row.total_gmv.toString(),
    }))

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

export const getAllTimeLeaders = async () => {
  try {
    const leaderboardData: Leaderboard[] = await prisma.$queryRaw`
  WITH valid_orders AS (
      SELECT order_id
      FROM public."orderData"
      GROUP BY order_id
      HAVING BOOL_AND(order_status <> 'cancelled')  -- Exclude orders where any entry is 'cancelled'
  )
  SELECT
      r.game_id,
      COALESCE(SUM(r.points), 0) AS total_points,
      COUNT(DISTINCT r.order_id)::BIGINT AS total_orders,  -- Only count valid order_ids
      COALESCE(SUM(r.gmv), 0)::BIGINT AS total_gmv
  FROM public."rewardledger" r 
  WHERE r.order_id IN (SELECT order_id FROM valid_orders)  -- Only include non-cancelled order_ids
  GROUP BY r.game_id
  ORDER BY total_points DESC;
`

    const updatedData = leaderboardData.map((row: Leaderboard) => ({
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
    console.error("Error fetching all time leaderboard data:", error)
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

export const DayWinnerUpdate = async () => await storePastWinners("daily_top_leaderboard", "daily")
export const WeeklyWinnerUpdate = async () => await storePastWinners("weekly_top_leaderboard", "weekly")
export const MonthlyWinnerUpdate = async () => await storePastWinners("monthly_top_leaderboard", "monthly")

const storePastWinners = async (leaderboardTable: string, type: string) => {
  try {
    const results: Leaderboard[] = await prisma.$queryRawUnsafe(
      `SELECT * FROM ${leaderboardTable} ORDER BY total_points DESC LIMIT 3`,
    )

    for (let i = 0; i < results.length; i++) {
      await prisma.dailyWinner.create({
        data: {
          game_id: results[i].game_id,
          points: results[i].total_points,
          position: i + 1,
          type: type,
          winning_date: new Date(),
        },
      })
    }
  } catch (error) {
    console.error(`Error storing ${type} winners:`, error)
  }
}

export const checkDailyWinnerCancellation = async () => {
  try {
    const previousDay = new Date()
    previousDay.setDate(previousDay.getDate() - 1)
    previousDay.setHours(0, 0, 0, 0)

    const previousDayEnd = new Date(previousDay)
    previousDayEnd.setHours(23, 59, 59, 999)

    const today = new Date()
    today.setHours(0, 0, 0, 0)

    const todayEnd = new Date(today)
    todayEnd.setHours(23, 59, 59, 999)

    const dailyWinner = await prisma.orderData.groupBy({
      by: ["uid"],
      _sum: { points: true },
      where: {
        timestamp_created: {
          gte: previousDay,
          lte: previousDayEnd,
        },
        highest_gmv_for_day: true,
      },
      orderBy: {
        _sum: { points: "desc" },
      },
      take: 2,
    })

    if (!dailyWinner.length) {
      console.log("No winner found for the previous day.")
      return
    }

    const winnerUid = dailyWinner[0].uid

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

export const checkWeeklyWinnerCancellation = async () => {
  try {
    const startOfWeek1 = new Date()
    startOfWeek1.setDate(startOfWeek1.getDate() - (startOfWeek1.getDay() + 7))
    startOfWeek1.setHours(0, 0, 0, 0)

    const endOfWeek1 = new Date(startOfWeek1)
    endOfWeek1.setDate(startOfWeek1.getDate() + 6)
    endOfWeek1.setHours(23, 59, 59, 999)

    const startOfWeek2 = new Date()
    startOfWeek2.setDate(startOfWeek2.getDate() - startOfWeek2.getDay())
    startOfWeek2.setHours(0, 0, 0, 0)

    const endOfWeek2 = new Date(startOfWeek2)
    endOfWeek2.setDate(startOfWeek2.getDate() + 6)
    endOfWeek2.setHours(23, 59, 59, 999)

    // Find the winner for the previous week (week_1)
    const weeklyWinner = await prisma.orderData.groupBy({
      by: ["uid"],
      _sum: { points: true },
      where: {
        timestamp_created: {
          gte: startOfWeek1,
          lte: endOfWeek1,
        },
        highest_gmv_for_day: true,
      },
      orderBy: {
        _sum: { points: "desc" },
      },
      take: 1,
    })

    if (!weeklyWinner.length) {
      console.log("No winner found for the previous week.")
      return
    }

    const winnerUid = weeklyWinner[0].uid

    // Check if the winner canceled any orders in Week 2 (this week)
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
      // If they have canceled orders in Week 2, remove their winner status for Week 1
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

      // Call handleOrderCancellationAndViolation to track violation and adjust points/status
      // await handleOrderCancellationAndViolation(winnerUid, "weekly", 1)
    } else {
      console.log(`Winner ${winnerUid} did not cancel orders in Week 2. Status remains for Week 1.`)
    }
  } catch (error) {
    console.error("Error checking weekly winner cancellations:", error)
  }
}

export const highestGmvandhighestOrder = async () => {
  try {
    const todayDate = new Date()
    // today highest order and highest gmv
    const highestOrders: any = await prisma.$queryRaw`
      WITH daily_orders AS (
    SELECT 
    game_id,
    DATE(order_timestamp_created) AS order_date,
    COUNT(DISTINCT CASE WHEN order_status = 'active' THEN order_id END)
      - COUNT(DISTINCT CASE WHEN order_status = 'cancelled' THEN order_id END) AS total_orders
FROM rewardledgertesting
WHERE 
    DATE(order_timestamp_created) = DATE(${todayDate})
GROUP BY game_id, DATE(order_timestamp_created)
ORDER BY total_orders desc limit 1
),

top_game_per_day AS (
    SELECT 
        game_id,
        order_date,
        total_orders
    FROM daily_orders
    ORDER BY total_orders DESC
    LIMIT 1
),

latest_order AS (
    SELECT 
        rl.order_id,
        rl.order_timestamp_created,
        rl.game_id,
        tgp.total_orders
    FROM rewardledgertesting rl
    JOIN top_game_per_day tgp
      ON rl.game_id = tgp.game_id
     AND DATE(rl.order_timestamp_created) = tgp.order_date
    WHERE rl.order_status != 'cancelled'
    ORDER BY rl.order_timestamp_created DESC
    LIMIT 1
)

SELECT * FROM latest_order
      `
    highestOrders.length > 0 &&
      insertrewardledgertesting(
        highestOrders[0]?.game_id,
        highestOrders[0]?.order_id,
        0,
        100,
        `${highestOrders[0]?.total_orders} with highest Orders in ${todayDate}`,
        "assigned",
        highestOrders[0]?.order_timestamp_created,
      )

    const highestGMV: any = await prisma.$queryRaw`
       WITH daily_orders AS  (  SELECT 
    game_id,
    DATE(order_timestamp_created) AS order_date,
   SUM(CASE WHEN order_status = 'active' OR order_status='assigned' THEN COALESCE(gmv, 0) ELSE 0 END)
      + SUM(CASE WHEN order_status = 'cancelled' THEN COALESCE(gmv, 0) ELSE 0 END) AS total_gmv
FROM rewardledgertesting
WHERE 
    DATE(order_timestamp_created) = DATE(${todayDate})
GROUP BY game_id, DATE(order_timestamp_created)
ORDER BY total_gmv desc limit 1)
,
top_game_per_day AS (
    SELECT 
        game_id,
        order_date,
        total_gmv
    FROM daily_orders
    ORDER BY total_gmv DESC
    LIMIT 1
),

latest_order AS (
    SELECT 
        rl.order_id,
        rl.order_timestamp_created,
        rl.game_id,
        tgp.total_gmv
    FROM rewardledgertesting rl
    JOIN top_game_per_day tgp
      ON rl.game_id = tgp.game_id
     AND DATE(rl.order_timestamp_created) = tgp.order_date
    WHERE rl.order_status != 'cancelled'
    ORDER BY rl.order_timestamp_created DESC
    LIMIT 1
)

SELECT * FROM latest_order
      `

    highestGMV.length > 0 &&
      insertrewardledgertesting(
        highestGMV[0]?.game_id,
        highestGMV[0]?.order_id,
        0,
        100,
        `${highestGMV[0]?.total_gmv} with highest gmv in ${todayDate}`,
        "assigned",
        highestGMV[0]?.order_timestamp_created,
      )
  } catch (error: any) {
    console.log("error", error)
  }
}

export const fetchLeaderboardForWeek = async (date: string) => {
  try {
    const startDate = new Date(date)
    const currentWeekStart = new Date(startDate)
    currentWeekStart.setDate(startDate.getDate() - startDate.getDay() + (startDate.getDay() === 0 ? -6 : 1)) // Monday of the week
    const currentWeekStartStr = currentWeekStart.toISOString().split("T")[0] // YYYY-MM-DD
    const endDate = new Date(currentWeekStart)
    endDate.setDate(currentWeekStart.getDate() + 7)
    const endDateStr = endDate.toISOString().split("T")[0]

    const leaderboard: Leaderboard[] = await prisma.$queryRaw`
      WITH valid_orders AS (
          SELECT order_id
          FROM public."orderData"
          GROUP BY order_id
          HAVING BOOL_AND(order_status <> 'cancelled')  -- Exclude orders where any entry is 'cancelled'
      )
      SELECT
          r.game_id,
          COALESCE(SUM(r.points), 0) AS total_points,
          COUNT(DISTINCT r.order_id)::BIGINT AS total_orders,  -- Only count valid order_ids
          COALESCE(SUM(r.gmv), 0)::BIGINT AS total_gmv,
          ${currentWeekStartStr}::DATE AS leaderboard_week_start
      FROM public."rewardledgertesting" r 
      WHERE DATE(r.created_at) >= ${currentWeekStartStr}::DATE
        AND DATE(r.created_at) < ${endDateStr}::DATE
        AND r.order_id IN (SELECT order_id FROM valid_orders)  -- Only include non-cancelled order_ids
      GROUP BY r.game_id
      ORDER BY total_points DESC;
    `

    const formattedLeaderboard = leaderboard.map((entry: Leaderboard) => ({
      ...entry,
      total_orders: Number(entry.total_orders),
      total_gmv: Number(entry.total_gmv),
    }))

    return {
      statusCode: 200,
      body: formattedLeaderboard,
    }
  } catch (error) {
    console.error("Error fetching weekly leaderboard:", error)
    return {
      statusCode: 500,
      body: "Internal Server Error",
    }
  }
}

export const getLeaderboardByDate = async (date: string) => {
  try {
    const startDate = new Date(date).toISOString().split("T")[0]

    const leaderboard: Leaderboard[] = await prisma.$queryRaw`
      WITH valid_orders AS (
          SELECT order_id
          FROM public."orderData"
          GROUP BY order_id
          HAVING BOOL_AND(order_status <> 'cancelled')
      )
      SELECT 
          r.game_id,
          COALESCE(SUM(r.points), 0) AS total_points,
          COUNT(DISTINCT r.order_id)::BIGINT AS total_orders,
          COALESCE(SUM(r.gmv), 0)::BIGINT AS total_gmv,
          ${startDate}::DATE AS leaderboard_day_start
      FROM public."rewardledgertesting" r
      WHERE DATE(r.created_at) = ${startDate}::DATE
        AND r.order_id IN (SELECT order_id FROM valid_orders)
      GROUP BY r.game_id
      ORDER BY total_points DESC;
    `

    const formattedLeaderboard = leaderboard.map((entry: Leaderboard) => ({
      ...entry,
      total_orders: Number(entry.total_orders),
      total_gmv: Number(entry.total_gmv),
    }))

    return {
      statusCode: 200,
      body: formattedLeaderboard,
    }
  } catch (error) {
    console.error("Error fetching leaderboard:", error)
    return {
      statusCode: 500,
      body: "Internal Server Error",
    }
  }
}

export const getMonthlyLeaderboardData = async () => {
  try {
    const leaderboardData: Leaderboard[] = await prisma.$queryRaw`
          SELECT * FROM monthly_top_leaderboard 
          ORDER BY total_points DESC;
        `

    const updatedData = leaderboardData.map((row: Leaderboard) => ({
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
