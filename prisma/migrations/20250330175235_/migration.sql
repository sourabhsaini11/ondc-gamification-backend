/*
  Warnings:

  - You are about to drop the column `buyer_app_id` on the `orderData` table. All the data in the column will be lost.
  - You are about to drop the column `domain` on the `orderData` table. All the data in the column will be lost.
  - You are about to drop the column `name` on the `orderData` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "orderData" DROP COLUMN "buyer_app_id",
DROP COLUMN "domain",
DROP COLUMN "name";

-- CreateTable
CREATE TABLE "rewardledgertesting" (
    "id" SERIAL NOT NULL,
    "order_id" TEXT NOT NULL DEFAULT '',
    "game_id" TEXT NOT NULL DEFAULT '',
    "points" DECIMAL(65,30) NOT NULL DEFAULT 0.00,
    "gmv" DECIMAL(65,30) NOT NULL DEFAULT 0.00,
    "reason" TEXT NOT NULL DEFAULT '',
    "order_status" TEXT NOT NULL DEFAULT '',
    "order_timestamp_created" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "rewardledgertesting_pkey" PRIMARY KEY ("id")
);
