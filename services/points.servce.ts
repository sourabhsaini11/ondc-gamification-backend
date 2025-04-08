import { PrismaClient } from "@prisma/client"
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


    const previewResults = await prisma.$executeRawUnsafe(`
            CREATE VIEW daily_top_leaderboard AS
            WITH valid_orders AS (
    SELECT order_id ,buyer_app_id
    FROM public."orderData"
    GROUP BY order_id ,buyer_app_id
    HAVING BOOL_AND(order_status <> 'cancelled')  -- Exclude orders where any entry is 'cancelled'
)
SELECT
    r.game_id,
    COALESCE(SUM(r.points), 0) AS total_points,
    COUNT(DISTINCT r.order_id)::BIGINT AS total_orders,  -- Count distinct order_ids
    COALESCE(SUM(r.gmv), 0)::BIGINT AS total_gmv,
    vo.buyer_app_id AS buyer_app_id,
    '${todayDate}'::DATE AS leaderboard_day_start
FROM public."rewardledgertesting" r
JOIN valid_orders vo ON vo.order_id = r.order_id
WHERE r.order_id IN (SELECT order_id FROM valid_orders)  -- Only include non-cancelled order_ids
GROUP BY r.game_id ,vo.buyer_app_id
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
    currentWeekStart.setDate(todayDate.getDate() - todayDate.getDay() + (todayDate.getDay() === 0 ? -6 : 1)) // Monday of this week
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
      }
    }

    await prisma.$executeRawUnsafe(`DROP VIEW IF EXISTS weekly_top_leaderboard;`)

    // Create or refresh the weekly leaderboard view
    //     const previewResults = await prisma.$executeRawUnsafe(`
    //      CREATE OR REPLACE VIEW weekly_top_leaderboard AS
    // WITH all_orders AS (
    //     -- Get all orders from the current week
    //     SELECT
    //         game_id,
    //         order_id,
    //         points,
    //         gmv,
    //         order_status,
    //         timestamp_created
    //     FROM public."orderData"
    //     WHERE timestamp_created >= DATE_TRUNC('week', '${currentWeekStartStr}'::TIMESTAMP)
    //       AND timestamp_created < DATE_TRUNC('week', '${currentWeekStartStr}'::TIMESTAMP) + INTERVAL '7 days'
    // ),
    // created_orders AS (
    //     -- Get orders where order_status is 'created'
    //     SELECT order_id, game_id, points, gmv
    //     FROM all_orders
    //     WHERE order_status = 'created'
    // ),
    // cancelled_orders AS (
    //     -- Get orders where order_status is 'cancelled'
    //     SELECT order_id, game_id
    //     FROM all_orders
    //     WHERE order_status = 'cancelled'
    // ),
    // valid_orders AS (  -- ✅ Define valid_orders
    //     SELECT
    //         c.game_id,
    //         c.order_id,
    //         c.points,
    //         c.gmv,
    //         1 AS valid_order  -- Mark these as valid orders
    //     FROM created_orders c
    //     LEFT JOIN cancelled_orders co ON c.order_id = co.order_id
    //     WHERE co.order_id IS NULL  -- Exclude cancelled orders
    // )
    // SELECT
    //     game_id,
    //     COUNT(valid_order) AS total_orders,  -- Count only valid orders
    //     SUM(points)::DOUBLE PRECISION AS total_points,
    //     SUM(gmv)::BIGINT AS total_gmv,
    //     DATE_TRUNC('week', '${currentWeekStartStr}'::TIMESTAMP)::DATE AS leaderboard_week_start
    // FROM valid_orders  -- ✅ Now valid_orders exists
    // GROUP BY game_id
    // HAVING SUM(points) >= 0  -- Exclude users with negative points
    // ORDER BY total_points DESC;

    //     `)

    //     const previewResults = await prisma.$executeRawUnsafe(`
    // CREATE OR REPLACE VIEW weekly_top_leaderboard AS
    //  WITH all_orders AS (
    //      -- Get all orders from the current week
    //      SELECT
    //          game_id,
    //          order_id,
    //          gmv,
    //          order_status,
    //          timestamp_created
    //      FROM public."orderData"
    //      WHERE timestamp_created >= DATE_TRUNC('week', '${currentWeekStartStr}'::TIMESTAMP)
    //        AND timestamp_created < DATE_TRUNC('week', '${currentWeekStartStr}'::TIMESTAMP) + INTERVAL '7 days'
    //  ),
    //  created_orders AS (
    //      -- Get orders where order_status is 'created'
    //      SELECT order_id, game_id, gmv
    //      FROM all_orders
    //      WHERE order_status = 'created'
    //  ),
    //  cancelled_orders AS (
    //      -- Get orders where order_status is 'cancelled'
    //      SELECT order_id, game_id
    //      FROM all_orders
    //      WHERE order_status = 'cancelled'
    //  ),
    //  valid_orders AS (
    //      -- Define valid orders by excluding cancelled ones
    //      SELECT
    //          c.game_id,
    //          c.order_id,
    //          c.gmv,
    //          1 AS valid_order  -- Mark these as valid orders
    //      FROM created_orders c
    //      LEFT JOIN cancelled_orders co ON c.order_id = co.order_id
    //      WHERE co.order_id IS NULL  -- Exclude cancelled orders
    //  ),
    //  rewardledger_points AS (
    //      -- Get the sum of points from the rewardLedger for the same period
    //      SELECT
    //          game_id,
    //          SUM(points)::BIGINT AS total_points  -- ✅ Explicitly cast to BIGINT
    //      FROM public."rewardledger"
    //      WHERE created_at >= DATE_TRUNC('week', '${currentWeekStartStr}'::TIMESTAMP)
    //        AND created_at < DATE_TRUNC('week','${currentWeekStartStr}'::TIMESTAMP) + INTERVAL '7 days'
    //      GROUP BY game_id
    //  )
    //  SELECT
    //      vo.game_id,  -- ✅ Fixed alias reference
    //      COUNT(vo.valid_order) AS total_orders,  -- Count only valid orders
    //      COALESCE(rp.total_points, 0) AS total_points,  -- ✅ No need to cast again, already BIGINT
    //      SUM(vo.gmv)::BIGINT AS total_gmv,
    //      DATE_TRUNC('week', '${currentWeekStartStr}'::TIMESTAMP)::DATE AS leaderboard_week_start
    //  FROM valid_orders vo
    //  LEFT JOIN rewardledger_points rp ON vo.game_id = rp.game_id  -- ✅ Join with rewardledger
    //  GROUP BY vo.game_id, rp.total_points
    //  HAVING COALESCE(rp.total_points, 0) >= 0  -- Exclude users with negative points
    //  ORDER BY total_points DESC;
    //     `)

    const previewResults = await prisma.$executeRawUnsafe(`
      CREATE VIEW weekly_top_leaderboard AS
      WITH valid_orders AS (
          SELECT order_id ,buyer_app_id
          FROM public."orderData"
          GROUP BY order_id ,buyer_app_id
          HAVING BOOL_AND(order_status <> 'cancelled')  -- Exclude orders where any entry is 'cancelled'
      )
      SELECT
          r.game_id,
          COALESCE(SUM(r.points), 0) AS total_points,
          COUNT(DISTINCT r.order_id)::BIGINT AS total_orders,  -- Only count valid order_ids
          COALESCE(SUM(r.gmv), 0)::BIGINT AS total_gmv,
          vo.buyer_app_id AS buyer_app_id ,
          '${currentWeekStartStr}'::DATE AS leaderboard_week_start
      FROM public."rewardledgertesting" r 
      JOIN valid_orders vo ON vo.order_id = r.order_id
      WHERE DATE(r.created_at) >= '${currentWeekStartStr}'::DATE
        AND r.order_id IN (SELECT order_id FROM valid_orders)  -- Only include non-cancelled order_ids
      GROUP BY r.game_id ,vo.buyer_app_id
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
    console.log("Today Date: ", todayDate)

    // Explicitly calculate the start of the current month (1st day of this month) in UTC
    const currentMonthStart = new Date(Date.UTC(todayDate.getUTCFullYear(), todayDate.getUTCMonth(), 1))
    console.log("Calculated Current Month Start (UTC): ", currentMonthStart)

    // Convert the calculated date to a string format YYYY-MM-DD
    const currentMonthStartStr = currentMonthStart.toISOString().split("T")[0] // YYYY-MM-DD
    console.log("Formatted Current Month Start (UTC): ", currentMonthStartStr)

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
        await prisma.$executeRawUnsafe(`ALTER VIEW monthly_top_leaderboard`)
      }
    }

    // Drop the view if it exists and recreate it with updated logic
    await prisma.$executeRawUnsafe(`DROP VIEW IF EXISTS monthly_top_leaderboard`)

    const previewResults = await prisma.$executeRawUnsafe(`
      CREATE OR REPLACE VIEW monthly_top_leaderboard AS
      WITH valid_orders AS (
          SELECT order_id ,buyer_app_id
          FROM public."orderData"
          GROUP BY order_id ,buyer_app_id
          HAVING BOOL_AND(order_status <> 'cancelled')  -- Exclude orders where any entry is 'cancelled'
      )
      SELECT
          r.game_id,
          COALESCE(SUM(r.points), 0) AS total_points,
          COUNT(DISTINCT r.order_id)::BIGINT AS total_orders,  -- Only count valid order_ids
          COALESCE(SUM(r.gmv), 0)::BIGINT AS total_gmv,
          vo.buyer_app_id AS buyer_app_id
          '${currentMonthStart.toISOString().split("T")[0]}'::DATE AS leaderboard_month_start
      FROM public."rewardledgertesting" r 
      JOIN valid_orders vo ON vo.order_id = r.order_id
      WHERE DATE(r.created_at) >= '${currentMonthStart.toISOString().split("T")[0]}'::DATE
        AND r.order_id IN (SELECT order_id FROM valid_orders)  -- Only include non-cancelled order_ids
      GROUP BY r.game_id , vo.buyer_app_id
      ORDER BY total_points DESC;
    `)

    console.log("Preview", previewResults)
    console.log(`Monthly leaderboard view updated for the month starting ${currentMonthStartStr}.`)

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
    console.log("updateddata", updatedData)
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

export const getAllTimeLeaders = async () => {
  try {
    const leaderboardData: any = await prisma.$queryRaw`
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
    console.error("Error fetching all time leaderboard data:", error)
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

export const getLeaderboardByDate = async (date: string) => {
  try {
    const startDate = new Date(date).toISOString().split("T")[0]

    const leaderboard: any = await prisma.$queryRaw`
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
      FROM public."rewardledger" r
      WHERE DATE(r.created_at) = ${startDate}::DATE
        AND r.order_id IN (SELECT order_id FROM valid_orders)
      GROUP BY r.game_id
      ORDER BY total_points DESC;
    `

    const formattedLeaderboard = leaderboard.map((entry: any) => ({
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

export const fetchLeaderboardForWeek = async (date: string) => {
  try {
    const startDate = new Date(date)
    const currentWeekStart = new Date(startDate)
    currentWeekStart.setDate(startDate.getDate() - startDate.getDay() + (startDate.getDay() === 0 ? -6 : 1)) // Monday of the week
    const currentWeekStartStr = currentWeekStart.toISOString().split("T")[0] // YYYY-MM-DD
    const endDate = new Date(currentWeekStart)
    endDate.setDate(currentWeekStart.getDate() + 7)
    const endDateStr = endDate.toISOString().split("T")[0]

    console.log(`Fetching leaderboard for the week starting: ${currentWeekStartStr}`)

    const leaderboard: any = await prisma.$queryRaw`
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
      FROM public."rewardledger" r 
      WHERE DATE(r.created_at) >= ${currentWeekStartStr}::DATE
        AND DATE(r.created_at) < ${endDateStr}::DATE
        AND r.order_id IN (SELECT order_id FROM valid_orders)  -- Only include non-cancelled order_ids
      GROUP BY r.game_id
      ORDER BY total_points DESC;
    `

    const formattedLeaderboard = leaderboard.map((entry: any) => ({
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
    const res = await prisma.$executeRawUnsafe(`
CREATE OR REPLACE FUNCTION update_leaderboard_manual()
  RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO leaderboard (game_id, total_points, total_orders, total_gmv)
    SELECT
        game_id,
        SUM(points),
        COUNT(order_id),
        SUM(gmv)
    FROM "orderData"
    GROUP BY game_id
    ON CONFLICT (game_id) 
    DO UPDATE SET 
        total_points = EXCLUDED.total_points,
        total_orders = EXCLUDED.total_orders,
        total_gmv = EXCLUDED.total_gmv;
END;
$$ LANGUAGE plpgsql;
        `)

    // await prisma.$executeRawUnsafe(`
    //       INSERT INTO "orderData" (game_id, points, gmv, order_status, timestamp_created)
    //       VALUES (123, 50, 1000, 'created', NOW());
    //     `)

    console.log("✅ Leaderboard update function created.", res)

    // Remove existing trigger if it exists
    // await prisma.$executeRawUnsafe(`
    //     DROP TRIGGER IF EXISTS update_leaderboard_trigger ON "orderData";
    //   `)

    console.log("🔄 Old leaderboard trigger removed (if it existed).")

    // Create the new trigger
    await prisma.$executeRawUnsafe(`
CREATE OR REPLACE FUNCTION update_leaderboard_trigger()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM update_leaderboard_manual();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
      `)

    console.log("✅ New leaderboard trigger created successfully.")
  } catch (error) {
    console.error("❌ Error setting up leaderboard trigger:", error)
  }
}

export const rewardLedgerTrigger = async () => {
  try {
    console.log("🔄 Setting up rewardledger trigger...")

    // Drop existing functions and triggers
    await prisma.$executeRawUnsafe(`DROP FUNCTION IF EXISTS update_leaderboard_manual CASCADE;`)
    await prisma.$executeRawUnsafe(`DROP FUNCTION IF EXISTS rewardledger_function CASCADE;`)
    await prisma.$executeRawUnsafe(`DROP TRIGGER IF EXISTS rewardledger_trigger ON "orderData";`)

    // Create or replace the function
    await prisma.$executeRawUnsafe(`
      CREATE OR REPLACE FUNCTION rewardledger_function()
RETURNS TRIGGER AS $$
DECLARE
    order_count INT;
BEGIN
    -- Calculate the order's position in the sequence for the day
    SELECT COUNT(*) INTO order_count
    FROM "orderData"
    WHERE game_id = NEW.game_id
      AND timestamp_created >= DATE(NEW.timestamp_created)
      AND timestamp_created < DATE(NEW.timestamp_created) + INTERVAL '1 day'
      AND order_status = 'created'
      AND (timestamp_created < NEW.timestamp_created OR 
          (timestamp_created = NEW.timestamp_created AND order_id <= NEW.order_id));

    -- Insert reward for the new order only
    -- INSERT INTO rewardledger (order_id, game_id, points, reason, updated_at)
    -- VALUES (NEW.order_id, NEW.game_id, order_count * 5, 'Daily order', NOW());

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
    `)

    console.log("✅ RewardLedger trigger function created successfully!")

    // Remove old rewardledger trigger if it exists
    await prisma.$executeRawUnsafe(`DROP TRIGGER IF EXISTS trigger_reward_ledger ON "orderData";`)
    console.log("🔄 Old rewardledger trigger removed (if it existed).")

    // Create the new rewardledger trigger
    await prisma.$executeRawUnsafe(`
      CREATE TRIGGER rewardledger_trigger
      BEFORE INSERT OR UPDATE OR DELETE ON "orderData"
      FOR EACH ROW
      EXECUTE FUNCTION rewardledger_function();
    `)

    console.log("✅ New rewardledger trigger created successfully.")
  } catch (error) {
    console.error("❌ Error setting up rewardledger trigger:", error)
  }
}

export const DayWinnerUpdate = async () => await storePastWinners("daily_top_leaderboard", "daily")
export const WeeklyWinnerUpdate = async () => await storePastWinners("weekly_top_leaderboard", "weekly")
export const MonthlyWinnerUpdate = async () => await storePastWinners("monthly_top_leaderboard", "monthly")

const storePastWinners = async (leaderboardTable: string, type: string) => {
  try {
    const results: any = await prisma.$queryRawUnsafe(
      `SELECT * FROM ${leaderboardTable} ORDER BY total_points DESC LIMIT 3`,
    )

    console.log("results", results)

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

    console.log(`${type} winners stored successfully`)
  } catch (error) {
    console.error(`Error storing ${type} winners:`, error)
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

export const highestGmvandOrder = async () => {
  try {
    //Today Winner
    const CurrentDayWinnerquery = `
    Select game_id ,SUM(gmv) from rewardledgertesting where order_status!= 'cancelled' and DATE(order_timestamp_created) = CURRENT_DATE GROUP BY game_id;
  `
    //Previous day Updated top two position players for comparing because may be if someone has cancelled order here we will get update points
    const PreviousDayWinnerquery = `
    SELECT 
        game_id, 
        SUM(gmv)
    FROM rewardledgertesting 
    WHERE DATE(order_timestamp_created) = CURRENT_DATE - INTERVAL '1 day'
    and order_status!= 'cancelled'
    GROUP BY game_id
  `
    const CurrentDayResult: any = await prisma.$queryRawUnsafe(CurrentDayWinnerquery)

    const PreviousDayResult: any = await prisma.$queryRawUnsafe(PreviousDayWinnerquery)

    //getting the winner of previous day that was already stored in DailyWinner
    const winnerStatus: any = await prisma.$queryRawUnsafe(`
        SELECT game_id
   FROM dailyWinner
   WHERE winning_date >= CURRENT_DATE - INTERVAL '1 day' -- Start of the previous day
     AND winning_date < CURRENT_DATE  and position=1 and type='daily' 
      `)

    console.log(
      "winnerStatus",
      winnerStatus,
      "PreviousDayResult",
      PreviousDayResult,
      "CurrentDayResult",
      CurrentDayResult,
    )

    if (winnerStatus.length > 0 && PreviousDayResult[0]?.game_id != winnerStatus[0]?.game_id) {
      // rewardledgerUpdate(
      //   PreviousDayResult[0]?.game_id,
      //   PreviousDayResult[0]?.last_order_id,
      //   0,
      //   -100,
      //   "Count of highest order changed after a day",
      //   false,
      //   new Date(),
      // )
      // rewardledgerUpdate(
      //   PreviousDayResult[1]?.game_id,
      //   PreviousDayResult[1]?.last_order_id,
      //   0,
      //   100,
      //   "Count of highest order changed after a day",
      //   true,
      //   new Date(),
      // )
    }

    // rewardledgerUpdate(
    //   CurrentDayResult[0]?.game_id,
    //   CurrentDayResult[0]?.last_order_id,
    //   0,
    //   100,
    //   "Points for highest order in a day",
    //   true,
    //   new Date(),
    // )
    // finding orders 
    const result = await prisma.$queryRaw`
  SELECT 
    game_id, 
    DATE (order_timestamp_created),
    COUNT(DISTINCT CASE WHEN order_status = 'active' THEN order_id END)
    - COUNT(DISTINCT CASE WHEN order_status = 'cancelled' THEN order_id END) AS order_count
  FROM public.rewardledgertesting 
  GROUP BY game_id , DATE(order_timestamp_created)
  ORDER BY order_count DESC;
`;

    console.log("result", result)
  } catch (error: any) {
    console.log("error", error)
  }
}

export const PointsAssignedforhighestGmv = async () => {
  try {
    const highestGmvfordayquery = `
    WITH gmv_summary AS (
    SELECT 
        game_id, 
        SUM(gmv) / 2 AS half_gmv, -- Calculate sum of GMV and divide by 2
        MAX(created_at) AS last_created_at, -- Get the latest created_at timestamp
        (ARRAY_AGG(order_id ORDER BY created_at DESC))[1] AS last_order_id -- Get the last order_id
    FROM rewardledger
    WHERE DATE(created_at) = CURRENT_DATE -- Filter for the current day
    GROUP BY game_id
),
ranked_gmv AS (
    SELECT 
        game_id, 
        half_gmv, 
        last_order_id, 
        last_created_at,
        DENSE_RANK() OVER (ORDER BY half_gmv DESC) AS rank -- Rank by half_gmv
    FROM gmv_summary
)
SELECT 
    game_id, 
    half_gmv, 
    last_order_id, 
    last_created_at
FROM ranked_gmv
WHERE rank = 1; -- Select the row(s) with the maximum half_gmv
    `
    const result: any = await prisma.$executeRawUnsafe(highestGmvfordayquery)
    console.log(result)
  } catch (error) {
    console.log(error)
  }
}

export const fetchLeaderboardForWeek2 = async (date: string) => {
  try {
    const startDate = new Date(date)
    const currentWeekStart = new Date(startDate)
    currentWeekStart.setDate(startDate.getDate() - startDate.getDay() + (startDate.getDay() === 0 ? -6 : 1)) // Monday of the week
    const currentWeekStartStr = currentWeekStart.toISOString().split("T")[0] // YYYY-MM-DD
    const endDate = new Date(currentWeekStart)
    endDate.setDate(currentWeekStart.getDate() + 7)
    const endDateStr = endDate.toISOString().split("T")[0]

    console.log(`Fetching leaderboard for the week starting: ${currentWeekStartStr}`)

    const leaderboard: any = await prisma.$queryRaw`
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

    const formattedLeaderboard = leaderboard.map((entry: any) => ({
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

export const getLeaderboardByDate2 = async (date: string) => {
  try {
    const startDate = new Date(date).toISOString().split("T")[0]

    const leaderboard: any = await prisma.$queryRaw`
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

    const formattedLeaderboard = leaderboard.map((entry: any) => ({
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

export const getMonthlyLeaderboardData2 = async () => {
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
