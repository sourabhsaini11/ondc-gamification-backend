/*
  Warnings:

  - You are about to drop the column `basePrice` on the `OrderData` table. All the data in the column will be lost.
  - You are about to drop the column `buyerAppId` on the `OrderData` table. All the data in the column will be lost.
  - You are about to drop the column `convenienceFee` on the `OrderData` table. All the data in the column will be lost.
  - You are about to drop the column `gameId` on the `OrderData` table. All the data in the column will be lost.
  - You are about to drop the column `highestGmvForDay` on the `OrderData` table. All the data in the column will be lost.
  - You are about to drop the column `highestOrdersForDay` on the `OrderData` table. All the data in the column will be lost.
  - You are about to drop the column `orderId` on the `OrderData` table. All the data in the column will be lost.
  - You are about to drop the column `orderStatus` on the `OrderData` table. All the data in the column will be lost.
  - You are about to drop the column `sellerId` on the `OrderData` table. All the data in the column will be lost.
  - You are about to drop the column `shippingCharges` on the `OrderData` table. All the data in the column will be lost.
  - You are about to drop the column `streakMaintain` on the `OrderData` table. All the data in the column will be lost.
  - You are about to drop the column `streakcount` on the `OrderData` table. All the data in the column will be lost.
  - You are about to drop the column `timestampCreated` on the `OrderData` table. All the data in the column will be lost.
  - You are about to drop the column `timestampUpdated` on the `OrderData` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "OrderData" DROP COLUMN "basePrice",
DROP COLUMN "buyerAppId",
DROP COLUMN "convenienceFee",
DROP COLUMN "gameId",
DROP COLUMN "highestGmvForDay",
DROP COLUMN "highestOrdersForDay",
DROP COLUMN "orderId",
DROP COLUMN "orderStatus",
DROP COLUMN "sellerId",
DROP COLUMN "shippingCharges",
DROP COLUMN "streakMaintain",
DROP COLUMN "streakcount",
DROP COLUMN "timestampCreated",
DROP COLUMN "timestampUpdated",
ADD COLUMN     "base_price" TEXT NOT NULL DEFAULT '0',
ADD COLUMN     "buyer_app_id" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "convenience_fee" TEXT NOT NULL DEFAULT '0',
ADD COLUMN     "game_id" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "highest_gmv_for_day" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "highest_orders_for_day" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "last_streak_count" TEXT NOT NULL DEFAULT '0',
ADD COLUMN     "order_id" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "order_status" TEXT NOT NULL DEFAULT 'Pending',
ADD COLUMN     "seller_id" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "shipping_charges" TEXT NOT NULL DEFAULT '0',
ADD COLUMN     "streak_count" TEXT NOT NULL DEFAULT '0',
ADD COLUMN     "streak_maintain" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "timestamp_created" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN     "timestamp_updated" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP;
