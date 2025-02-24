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
    const todayDate = new Date().toISOString().split("T")[0]

    const orderCheck = await prisma.$queryRaw`
    SELECT COUNT(*) AS order_count 
    FROM "orderData" 
    WHERE timestamp_created >= '2025-02-05'::DATE 
AND timestamp_created < ('2025-02-05'::DATE + INTERVAL '1 day');
  `

    console.log("Orders found for", todayDate, ":", orderCheck)

    // Create or replace the daily leaderboard view
    await prisma.$executeRawUnsafe(`DROP VIEW IF EXISTS daily_top_leaderboard;`)

    // Create the new daily leaderboard view
    const previewResults = await prisma.$executeRawUnsafe(`
        CREATE VIEW daily_top_leaderboard AS
        WITH order_points AS (
            SELECT 
                game_id,
                order_id,
                SUM(points) AS total_order_points,
                SUM(gmv) AS total_order_gmv,
                COUNT(CASE WHEN order_status NOT IN ('cancelled', 'partially_cancelled') THEN 1 END) AS valid_orders
            FROM public."orderData"
            WHERE timestamp_created >= '${todayDate}'::DATE
            AND timestamp_created < ('${todayDate}'::DATE + INTERVAL '1 day')
            GROUP BY game_id, order_id
        )
        SELECT 
            game_id,
            SUM(total_order_points) AS total_points,
            SUM(valid_orders) AS total_orders,
            SUM(total_order_gmv) AS total_gmv
        FROM order_points
        GROUP BY game_id
        HAVING SUM(total_order_points) > 0
        ORDER BY total_points DESC;
    `)

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
        SELECT 
            game_id, 
            SUM(points) AS total_points, 
            COUNT(order_id) AS total_orders, 
            SUM(gmv) AS total_gmv, 
            '${currentWeekStart.toISOString().split("T")[0]}'::DATE AS leaderboard_week_start
        FROM public."orderData"
        WHERE timestamp_created >= '${currentWeekStart.toISOString().split("T")[0]}'::DATE
        GROUP BY game_id
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
    const previewResults = await prisma.$executeRawUnsafe(`
        CREATE OR REPLACE VIEW monthly_top_leaderboard AS
        SELECT 
            game_id, 
            SUM(points)::DOUBLE PRECISION AS total_points,  -- Ensure total_points is FLOAT
            COUNT(order_id)::BIGINT AS total_orders,  -- Ensure total_orders remains BIGINT
            SUM(gmv)::BIGINT AS total_gmv,  -- Ensure total_gmv is BIGINT
            '${currentMonthStart.toISOString().split("T")[0]}'::DATE AS leaderboard_month_start

        FROM public."orderData"
        WHERE DATE(timestamp_created) >= '${currentMonthStart.toISOString().split("T")[0]}'::DATE
        GROUP BY game_id
        ORDER BY total_points DESC;
    `)

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
