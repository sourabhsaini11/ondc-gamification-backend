/*
  Warnings:

  - The `streak_count` column on the `OrderData` table would be dropped and recreated. This will lead to data loss if there is data in the column.

*/
-- AlterTable
ALTER TABLE "OrderData" DROP COLUMN "streak_count",
ADD COLUMN     "streak_count" INTEGER NOT NULL DEFAULT 0;
