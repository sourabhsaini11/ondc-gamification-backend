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
    const todayDate = "2025-02-05" // Just the date, no timestamp

    const orderCheck = await prisma.$queryRaw`
    SELECT COUNT(*) AS order_count 
    FROM "orderData" 
    WHERE timestamp_created >= '2025-02-05'::DATE 
AND timestamp_created < ('2025-02-05'::DATE + INTERVAL '1 day');
  `

    console.log("Orders found for", todayDate, ":", orderCheck)

    // Create or replace the daily leaderboard view
    const previewResults = await prisma.$executeRawUnsafe(
      `
            CREATE OR REPLACE VIEW daily_top_leaderboard AS
            WITH order_points AS (
                SELECT 
                    game_id,
                    order_id,
                    SUM(points) AS total_order_points,
                    SUM(gmv) AS total_order_gmv,
                    COUNT(CASE WHEN order_status NOT IN ('cancelled', 'partially_cancelled') THEN 1 END) AS valid_orders
                FROM "orderData"
                WHERE timestamp_created >= '${todayDate}'::DATE
                AND timestamp_created < ('${todayDate}'::DATE + INTERVAL '1 day')
                GROUP BY game_id, order_id
            )
            SELECT 
                game_id,
                SUM(total_order_points) AS total_points,
                SUM(valid_orders) AS total_orders,
                SUM(total_order_gmv) AS total_gmv,
            FROM order_points
            GROUP BY game_id
            HAVING SUM(total_order_points) > 0
            ORDER BY total_points DESC;
        `,
    )

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
    await prisma.$executeRawUnsafe(`
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

    console.log("✅ Leaderboard update function created.")

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
