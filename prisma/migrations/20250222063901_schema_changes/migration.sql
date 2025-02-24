/*
  Warnings:

  - The `timestampCreated` column on the `OrderData` table would be dropped and recreated. This will lead to data loss if there is data in the column.
  - The `timestampUpdated` column on the `OrderData` table would be dropped and recreated. This will lead to data loss if there is data in the column.
  - Added the required column `last_streak_date` to the `Leaderboard` table without a default value. This is not possible if the table is not empty.
  - Added the required column `streak` to the `Leaderboard` table without a default value. This is not possible if the table is not empty.
  - Added the required column `streak_maintain` to the `Leaderboard` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "Leaderboard" ADD COLUMN     "last_streak_date" TIMESTAMP(3) NOT NULL,
ADD COLUMN     "streak" INTEGER NOT NULL,
ADD COLUMN     "streak_maintain" BOOLEAN NOT NULL;

-- AlterTable
ALTER TABLE "OrderData" ADD COLUMN     "gameId" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "gmv" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "highestGmvForDay" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "highestOrdersForDay" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "points" TEXT NOT NULL DEFAULT '0',
ADD COLUMN     "streakMaintain" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "streakcount" TEXT NOT NULL DEFAULT '0',
DROP COLUMN "timestampCreated",
ADD COLUMN     "timestampCreated" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
DROP COLUMN "timestampUpdated",
ADD COLUMN     "timestampUpdated" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP;
