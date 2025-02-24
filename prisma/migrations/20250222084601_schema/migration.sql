/*
  Warnings:

  - You are about to drop the column `last_streak_count` on the `OrderData` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "OrderData" DROP COLUMN "last_streak_count",
ADD COLUMN     "last_streak_date" TEXT NOT NULL DEFAULT '0';
