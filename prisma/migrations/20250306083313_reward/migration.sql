/*
  Warnings:

  - You are about to alter the column `points` on the `rewardledger` table. The data in that column could be lost. The data in that column will be cast from `DoublePrecision` to `Decimal(65,30)`.

*/
-- AlterTable
ALTER TABLE "rewardledger" ALTER COLUMN "points" SET DATA TYPE DECIMAL(65,30);
