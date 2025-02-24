/*
  Warnings:

  - You are about to drop the `OrderData` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropTable
DROP TABLE "OrderData";

-- CreateTable
CREATE TABLE "orderData" (
    "id" SERIAL NOT NULL,
    "uid" TEXT NOT NULL DEFAULT '',
    "name" TEXT NOT NULL,
    "order_id" TEXT NOT NULL DEFAULT '',
    "order_status" TEXT NOT NULL DEFAULT 'Pending',
    "timestamp_created" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "timestamp_updated" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "category" TEXT NOT NULL DEFAULT '',
    "buyer_app_id" TEXT NOT NULL DEFAULT '',
    "base_price" DOUBLE PRECISION NOT NULL DEFAULT 0.00,
    "shipping_charges" DOUBLE PRECISION NOT NULL DEFAULT 0.00,
    "taxes" DOUBLE PRECISION NOT NULL DEFAULT 0.00,
    "discount" DOUBLE PRECISION NOT NULL DEFAULT 0.00,
    "convenience_fee" DOUBLE PRECISION NOT NULL DEFAULT 0.00,
    "seller_id" TEXT NOT NULL DEFAULT '',
    "streak_count" INTEGER NOT NULL DEFAULT 0,
    "last_streak_date" TIMESTAMP(3) NOT NULL,
    "updated_by_lambda" TIMESTAMP(3) NOT NULL,
    "game_id" TEXT NOT NULL DEFAULT '',
    "points" DOUBLE PRECISION NOT NULL DEFAULT 0.00,
    "streak_maintain" BOOLEAN NOT NULL DEFAULT false,
    "entry_updated" BOOLEAN NOT NULL DEFAULT false,
    "highest_gmv_for_day" BOOLEAN NOT NULL DEFAULT false,
    "highest_orders_for_day" BOOLEAN NOT NULL DEFAULT false,
    "gmv" INTEGER NOT NULL DEFAULT 0,

    CONSTRAINT "orderData_pkey" PRIMARY KEY ("id")
);
