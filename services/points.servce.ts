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
CREATE OR REPLACE VIEW daily_top_leaderboard AS
WITH all_orders AS (
    -- Get all orders from the current day
    SELECT 
        game_id,
        order_id,
        points,
        gmv,
        order_status,
        timestamp_created
    FROM public."orderData"
    WHERE timestamp_created >= DATE_TRUNC('day', CURRENT_DATE::TIMESTAMP)
      AND timestamp_created < DATE_TRUNC('day', CURRENT_DATE::TIMESTAMP) + INTERVAL '1 day'
),
created_orders AS (
    -- Get orders where order_status is 'created'
    SELECT order_id, game_id, points, gmv
    FROM all_orders
    WHERE order_status = 'created'
),
cancelled_orders AS (
    -- Get orders where order_status is 'cancelled'
    SELECT order_id, game_id
    FROM all_orders
    WHERE order_status = 'cancelled'
)
SELECT 
    co.game_id,
    -- Calculate total orders as created orders minus cancelled orders
    COUNT(DISTINCT co.order_id) - COUNT(DISTINCT ca.order_id) AS total_orders,  
    SUM(co.points)::DOUBLE PRECISION AS total_points,  
    SUM(co.gmv)::BIGINT AS total_gmv,
    DATE_TRUNC('day', CURRENT_DATE)::DATE AS leaderboard_day_start
FROM created_orders co
LEFT JOIN cancelled_orders ca ON co.order_id = ca.order_id
GROUP BY co.game_id
HAVING SUM(co.points) >= 0  -- Exclude users with negative points
ORDER BY total_points DESC;
          `)
    //     const previewResults = await prisma.$executeRawUnsafe(`
    //       CREATE OR REPLACE VIEW daily_top_leaderboard AS
    // WITH all_orders AS (
    //     -- Get all orders from the current day
    //     SELECT
    //         game_id,
    //         order_id,
    //         points,
    //         gmv,
    //         order_status,
    //         timestamp_created
    //     FROM public."orderData"
    //     WHERE timestamp_created >= DATE_TRUNC('day', CURRENT_DATE::TIMESTAMP)
    //       AND timestamp_created < DATE_TRUNC('day', CURRENT_DATE::TIMESTAMP) + INTERVAL '1 day'
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
    // )
    // SELECT
    //     co.game_id,
    //     -- Calculate total orders as created orders minus cancelled orders
    //     COUNT(DISTINCT co.order_id) - COUNT(DISTINCT ca.order_id) AS total_orders,
    //     SUM(co.points)::DOUBLE PRECISION AS total_points,
    //     SUM(co.gmv)::BIGINT AS total_gmv,
    //     DATE_TRUNC('day', CURRENT_DATE)::DATE AS leaderboard_day_start
    // FROM created_orders co
    // LEFT JOIN cancelled_orders ca ON co.order_id = ca.order_id
    // GROUP BY co.game_id
    // HAVING SUM(co.points) >= 0  -- Exclude users with negative points
    // ORDER BY total_points DESC;
    //           `)
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
 WITH all_orders AS (
     -- Get all orders from the current week
     SELECT 
         game_id,
         order_id,
         gmv,
         order_status,
         timestamp_created
     FROM public."orderData"
     WHERE timestamp_created >= DATE_TRUNC('week', '${currentWeekStartStr}'::TIMESTAMP)
       AND timestamp_created < DATE_TRUNC('week', '${currentWeekStartStr}'::TIMESTAMP) + INTERVAL '7 days'
 ),
 created_orders AS (
     -- Get orders where order_status is 'created'
     SELECT order_id, game_id, gmv
     FROM all_orders
     WHERE order_status = 'created'
 ),
 cancelled_orders AS (
     -- Get orders where order_status is 'cancelled'
     SELECT order_id, game_id
     FROM all_orders
     WHERE order_status = 'cancelled'
 ),
 valid_orders AS (  
     -- Define valid orders by excluding cancelled ones
     SELECT 
         c.game_id,
         c.order_id,
         c.gmv,
         1 AS valid_order  -- Mark these as valid orders
     FROM created_orders c
     LEFT JOIN cancelled_orders co ON c.order_id = co.order_id
     WHERE co.order_id IS NULL  -- Exclude cancelled orders
 ),
 rewardledger_points AS (
     -- Get the sum of points from the rewardLedger for the same period
     SELECT 
         game_id,
         SUM(points)::BIGINT AS total_points  -- ✅ Explicitly cast to BIGINT
     FROM public."rewardledger"
     WHERE created_at >= DATE_TRUNC('week', '${currentWeekStartStr}'::TIMESTAMP)
       AND created_at < DATE_TRUNC('week','${currentWeekStartStr}'::TIMESTAMP) + INTERVAL '7 days'
     GROUP BY game_id
 )
 SELECT 
     vo.game_id,  -- ✅ Fixed alias reference
     COUNT(vo.valid_order) AS total_orders,  -- Count only valid orders
     COALESCE(rp.total_points, 0) AS total_points,  -- ✅ No need to cast again, already BIGINT
     SUM(vo.gmv)::BIGINT AS total_gmv,
     DATE_TRUNC('week', '${currentWeekStartStr}'::TIMESTAMP)::DATE AS leaderboard_week_start
 FROM valid_orders vo
 LEFT JOIN rewardledger_points rp ON vo.game_id = rp.game_id  -- ✅ Join with rewardledger
 GROUP BY vo.game_id, rp.total_points
 HAVING COALESCE(rp.total_points, 0) >= 0  -- Exclude users with negative points
 ORDER BY total_points DESC;
    `)
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
    // valid_orders AS (  --
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
    //     SUM(points)::DOUBLE PRECISION AS total_points,
    //     COUNT(valid_order) AS total_orders,  -- Count only valid orders
    //     SUM(gmv)::BIGINT AS total_gmv,
    //     DATE_TRUNC('week', '${currentWeekStartStr}'::TIMESTAMP)::DATE AS leaderboard_week_start
    // FROM valid_orders  --
    // GROUP BY game_id
    // HAVING SUM(points) >= 0  -- Exclude users with negative points
    // ORDER BY total_points DESC;

    //     `)

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
        await prisma.$executeRawUnsafe(`ALTER VIEW monthly_top_leaderboard`)
      }
    }

    // Create or refresh the monthly leaderboard view
    await prisma.$executeRawUnsafe(`DROP VIEW IF EXISTS monthly_top_leaderboard`)
    // const previewResults = await prisma.$executeRawUnsafe(`
    //     CREATE OR REPLACE VIEW monthly_top_leaderboard AS
    //     SELECT
    //         game_id,
    //         SUM(points)::DOUBLE PRECISION AS total_points,  -- Ensure total_points is FLOAT
    //         COUNT(order_id)::BIGINT AS total_orders,  -- Ensure total_orders remains BIGINT
    //         SUM(gmv)::BIGINT AS total_gmv,  -- Ensure total_gmv is BIGINT
    //         '${currentMonthStart.toISOString().split("T")[0]}'::DATE AS leaderboard_month_start
    //     FROM public."orderData"
    //     WHERE DATE(timestamp_created) >= '${currentMonthStart.toISOString().split("T")[0]}'::DATE
    //     GROUP BY game_id
    //     ORDER BY total_points DESC;
    // `)

    const previewResults = await prisma.$executeRawUnsafe(`
      CREATE OR REPLACE VIEW monthly_top_leaderboard AS
      WITH valid_orders AS (
          SELECT order_id
          FROM public."orderData"
          GROUP BY order_id
          HAVING BOOL_AND(order_status <> 'cancelled')  -- Exclude orders where any entry is 'cancelled'
      )
      SELECT
          o.game_id,
          COALESCE(SUM(r.points), 0) AS total_points,
          COUNT(DISTINCT o.order_id)::BIGINT AS total_orders,  -- Only count valid order_ids
          COALESCE(SUM(r.gmv), 0)::BIGINT AS total_gmv,
          '${currentMonthStart.toISOString().split("T")[0]}'::DATE AS leaderboard_month_start
      FROM public."orderData" o
      LEFT JOIN public."rewardledger" r ON o.order_id = r.order_id
      WHERE DATE(o.timestamp_created) >= '${currentMonthStart.toISOString().split("T")[0]}'::DATE
        AND o.order_id IN (SELECT order_id FROM valid_orders)  -- Only include non-cancelled order_ids
      GROUP BY o.game_id
      ORDER BY total_points DESC;
    `)

    // const previewResults = await prisma.$executeRawUnsafe(`
    //   CREATE OR REPLACE VIEW monthly_top_leaderboard AS
    //   SELECT
    //       o.game_id,
    //       COALESCE(SUM(r.points), 0) AS total_points,  -- Sum points from rewardledger only
    //       COUNT(DISTINCT CASE WHEN o.order_status <> 'cancelled' THEN o.order_id END)::BIGINT AS total_orders,  -- Count distinct order_id to avoid double-counting
    //       COALESCE(SUM(o.gmv), 0)::BIGINT AS total_gmv,  -- Total GMV from orderData
    //       '${currentMonthStart.toISOString().split("T")[0]}'::DATE AS leaderboard_month_start
    //   FROM public."orderData" o
    //   LEFT JOIN public."rewardledger" r ON o.order_id = r.order_id  -- Join on order_id to ensure accurate rewardLedger points
    //   WHERE DATE(o.timestamp_created) >= '${currentMonthStart.toISOString().split("T")[0]}'::DATE
    //   GROUP BY o.game_id
    //   ORDER BY total_points DESC;
    // `)

    //

    // const previewResults = await prisma.$executeRawUnsafe(`
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

    //

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

// export const rewardLedgerTrigger = async () => {
//   try {
//     console.log("🔄 Setting up rewardledger trigger...")
//     await prisma.$executeRawUnsafe(`DROP FUNCTION IF EXISTS update_leaderboard_manual CASCADE;`)
//     await prisma.$executeRawUnsafe(`DROP FUNCTION IF EXISTS rewardledger_function CASCADE;`)
//     await prisma.$executeRawUnsafe(`DROP TRIGGER IF EXISTS rewardledger_trigger ON "orderData";`)
//     // Create or replace the function
//     const res = await prisma.$executeRawUnsafe(`
//  CREATE OR REPLACE FUNCTION rewardledger_function()
// RETURNS TRIGGER AS $$
// DECLARE
//     order_count INT;
//     streak_days INT;
//     streak_bonus INT;
//     order_points DECIMAL;
//     streakBonuses JSONB;
//     current_game_id TEXT;
//     current_order_count INT;
//     last_order_date DATE;
//     is_cancelled BOOLEAN;
//     highest_order_user TEXT;
//     highest_gmv_user TEXT;
//     previous_highest_order_user TEXT;
//     previous_highest_gmv_user TEXT;
//     order_data RECORD;  -- Declare the loop variable as a RECORD
// BEGIN
//     -- Streak bonuses map for consecutive days
//     streakBonuses := '{"3": 20, "7": 30, "10": 100, "14": 200, "21": 500, "28": 700}';

//     -- Get the game_id and order status for the current order
//     current_game_id := NEW.game_id;
//     is_cancelled := NEW.order_status = 'cancelled';  -- Only fully cancelled orders deduct points

//     -- Count the number of non-cancelled orders for this game_id on the current day
//     SELECT COUNT(*)
//     INTO current_order_count
//     FROM "orderData"
//     WHERE game_id = current_game_id
//       AND DATE(timestamp_created) = DATE(NEW.timestamp_created)
//       AND order_status != 'cancelled';  -- Exclude fully cancelled orders

//     -- Assign points based on the order number in the day (5 points per order)
//     order_points := current_order_count * 5;

//     -- If the order is cancelled, recalculate points for all orders on the same day
//     IF is_cancelled THEN
//         -- Deduct points for the cancelled order
//         INSERT INTO rewardledger (order_id, game_id, points, reason, updated_at)
//         VALUES (NEW.order_id, current_game_id, -5 * current_order_count, 'Order cancelled - points deducted', NOW());

//         -- Recalculate points for all remaining orders on the same day
//         FOR order_data IN
//             SELECT order_id
//             FROM "orderData"
//             WHERE game_id = current_game_id
//               AND DATE(timestamp_created) = DATE(NEW.timestamp_created)
//               AND order_status != 'cancelled'
//         LOOP
//             -- Recalculate points for each order
//             order_points := (SELECT COUNT(*)
//                             FROM "orderData"
//                             WHERE game_id = current_game_id
//                               AND DATE(timestamp_created) = DATE(NEW.timestamp_created)
//                               AND order_status != 'cancelled'
//                               AND order_id <= order_data.order_id) * 5;

//             -- Update the rewardledger entry for the order
//             UPDATE rewardledger
//             SET points = order_points
//             WHERE order_id = order_data.order_id;
//         END LOOP;

//         -- Recalculate the highest number of orders and GMV for the day
//         WITH order_counts AS (
//             SELECT game_id, COUNT(*) AS order_count
//             FROM "orderData"
//             WHERE DATE(timestamp_created) = DATE(NEW.timestamp_created)
//               AND order_status != 'cancelled'
//             GROUP BY game_id
//         ),
//         gmv_totals AS (
//             SELECT game_id, SUM(base_price) AS total_gmv
//             FROM "orderData"
//             WHERE DATE(timestamp_created) = DATE(NEW.timestamp_created)
//               AND order_status != 'cancelled'
//             GROUP BY game_id
//         )
//         SELECT
//             (SELECT game_id FROM order_counts ORDER BY order_count DESC LIMIT 1),
//             (SELECT game_id FROM gmv_totals ORDER BY total_gmv DESC LIMIT 1)
//         INTO highest_order_user, highest_gmv_user;

//         -- Deduct 100 points from the previous highest order user (if any)
//         SELECT game_id
//         INTO previous_highest_order_user
//         FROM rewardledger
//         WHERE reason = 'Highest number of orders in a day'
//           AND DATE(updated_at) = DATE(NEW.timestamp_created);

//         IF previous_highest_order_user IS NOT NULL AND previous_highest_order_user != highest_order_user THEN
//             INSERT INTO rewardledger (order_id, game_id, points, reason, updated_at)
//             VALUES (NEW.order_id, previous_highest_order_user, -100, 'Lost highest number of orders in a day', NOW());
//         END IF;

//         -- Award 100 points to the new highest order user
//         INSERT INTO rewardledger (order_id, game_id, points, reason, updated_at)
//         VALUES (NEW.order_id, highest_order_user, 100, 'Highest number of orders in a day', NOW());

//         -- Deduct 100 points from the previous highest GMV user (if any)
//         SELECT game_id
//         INTO previous_highest_gmv_user
//         FROM rewardledger
//         WHERE reason = 'Highest GMV in a day'
//           AND DATE(updated_at) = DATE(NEW.timestamp_created);

//         IF previous_highest_gmv_user IS NOT NULL AND previous_highest_gmv_user != highest_gmv_user THEN
//             INSERT INTO rewardledger (order_id, game_id, points, reason, updated_at)
//             VALUES (NEW.order_id, previous_highest_gmv_user, -100, 'Lost highest GMV in a day', NOW());
//         END IF;

//         -- Award 100 points to the new highest GMV user
//         INSERT INTO rewardledger (order_id, game_id, points, reason, updated_at)
//         VALUES (NEW.order_id, highest_gmv_user, 100, 'Highest GMV in a day', NOW());
//     ELSE
//         -- Insert into rewardledger for the current order
//         INSERT INTO rewardledger (order_id, game_id, points, reason, updated_at)
//         VALUES (NEW.order_id, current_game_id, order_points, 'Order placed - points assigned based on order count', NOW());
//     END IF;

//     -- Check streak (3+ consecutive days)
//     -- Get the last non-cancelled order date for this game_id
//     SELECT MAX(DATE(timestamp_created))
//     INTO last_order_date
//     FROM "orderData"
//     WHERE game_id = current_game_id
//       AND order_status != 'cancelled'
//       AND DATE(timestamp_created) < DATE(NEW.timestamp_created);

//     -- Calculate streak days
//     -- Calculate streak days
// IF last_order_date IS NOT NULL AND last_order_date = DATE(NEW.timestamp_created) - INTERVAL '1 day' THEN
//     -- Increment streak
//     streak_days := (SELECT COALESCE(MAX(streak_days), 0) + 1
//                    FROM rewardledger
//                    WHERE game_id = current_game_id
//                      AND reason LIKE 'Streak bonus%'
//                      AND updated_at >= CURRENT_DATE - INTERVAL '28 days');
// ELSE
//     -- Reset streak
//     streak_days := 1;
// END IF;

//     RAISE NOTICE 'Streak days: %', streak_days;

//     -- If a streak is maintained, give bonus points based on the streak
//     IF streak_days >= 3 THEN
//         -- Determine the streak bonus based on consecutive days
//         IF streak_days >= 28 THEN
//             streak_bonus := (streakBonuses ->> '28')::INT;
//         ELSIF streak_days >= 21 THEN
//             streak_bonus := (streakBonuses ->> '21')::INT;
//         ELSIF streak_days >= 14 THEN
//             streak_bonus := (streakBonuses ->> '14')::INT;
//         ELSIF streak_days >= 10 THEN
//             streak_bonus := (streakBonuses ->> '10')::INT;
//         ELSIF streak_days >= 7 THEN
//             streak_bonus := (streakBonuses ->> '7')::INT;
//         ELSIF streak_days >= 3 THEN
//             streak_bonus := (streakBonuses ->> '3')::INT;
//         END IF;

//         -- Insert streak bonus points into rewardledger
//         INSERT INTO rewardledger (order_id, game_id, points, reason, updated_at)
//         VALUES (NEW.order_id, current_game_id, streak_bonus, 'Streak bonus - consecutive days', NOW());
//     END IF;

//     RETURN NEW;
// END;
// $$ LANGUAGE plpgsql;
//     `)

//     console.log("✅ RewardLedger trigger function created successfully!", res)

//     // Remove old rewardledger trigger if it exists
//     await prisma.$executeRawUnsafe(`
//       DROP TRIGGER IF EXISTS trigger_reward_ledger ON "orderData";
//     `)
//     console.log("🔄 Old rewardledger trigger removed (if it existed).")

//     // Create the new rewardledger trigger
//     await prisma.$executeRawUnsafe(`
//       CREATE TRIGGER rewardledger_trigger
//       BEFORE INSERT OR UPDATE OR DELETE ON "orderData"
//       FOR EACH ROW
//       EXECUTE FUNCTION rewardledger_function();
//     `)
//     console.log("✅ New rewardledger trigger created successfully.")
//   } catch (error) {
//     console.error("❌ Error setting up rewardledger trigger:", error)
//   }
// }

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
