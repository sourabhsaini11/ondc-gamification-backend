/*
  Warnings:

  - You are about to drop the `Leaderboard` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropTable
DROP TABLE "Leaderboard";

-- CreateTable
CREATE TABLE "leaderboard" (
    "id" SERIAL NOT NULL,
    "game_id" TEXT NOT NULL DEFAULT '',
    "total_points" INTEGER NOT NULL,
    "total_orders" INTEGER NOT NULL,
    "total_gmv" INTEGER NOT NULL,

    CONSTRAINT "leaderboard_pkey" PRIMARY KEY ("id")
);
