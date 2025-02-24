/*
  Warnings:

  - The `taxes` column on the `OrderData` table would be dropped and recreated. This will lead to data loss if there is data in the column.
  - The `discount` column on the `OrderData` table would be dropped and recreated. This will lead to data loss if there is data in the column.
  - The `gmv` column on the `OrderData` table would be dropped and recreated. This will lead to data loss if there is data in the column.
  - The `points` column on the `OrderData` table would be dropped and recreated. This will lead to data loss if there is data in the column.
  - The `base_price` column on the `OrderData` table would be dropped and recreated. This will lead to data loss if there is data in the column.
  - The `convenience_fee` column on the `OrderData` table would be dropped and recreated. This will lead to data loss if there is data in the column.
  - The `highest_gmv_for_day` column on the `OrderData` table would be dropped and recreated. This will lead to data loss if there is data in the column.
  - The `highest_orders_for_day` column on the `OrderData` table would be dropped and recreated. This will lead to data loss if there is data in the column.
  - The `shipping_charges` column on the `OrderData` table would be dropped and recreated. This will lead to data loss if there is data in the column.
  - The `streak_maintain` column on the `OrderData` table would be dropped and recreated. This will lead to data loss if there is data in the column.
  - Changed the type of `last_streak_date` on the `OrderData` table. No cast exists, the column would be dropped and recreated, which cannot be done if there is data, since the column is required.

*/
-- AlterTable
ALTER TABLE "OrderData" DROP COLUMN "taxes",
ADD COLUMN     "taxes" DOUBLE PRECISION NOT NULL DEFAULT 0.00,
DROP COLUMN "discount",
ADD COLUMN     "discount" DOUBLE PRECISION NOT NULL DEFAULT 0.00,
DROP COLUMN "gmv",
ADD COLUMN     "gmv" INTEGER NOT NULL DEFAULT 0,
DROP COLUMN "points",
ADD COLUMN     "points" DOUBLE PRECISION NOT NULL DEFAULT 0.00,
DROP COLUMN "base_price",
ADD COLUMN     "base_price" DOUBLE PRECISION NOT NULL DEFAULT 0.00,
DROP COLUMN "convenience_fee",
ADD COLUMN     "convenience_fee" DOUBLE PRECISION NOT NULL DEFAULT 0.00,
DROP COLUMN "highest_gmv_for_day",
ADD COLUMN     "highest_gmv_for_day" BOOLEAN NOT NULL DEFAULT false,
DROP COLUMN "highest_orders_for_day",
ADD COLUMN     "highest_orders_for_day" BOOLEAN NOT NULL DEFAULT false,
DROP COLUMN "shipping_charges",
ADD COLUMN     "shipping_charges" DOUBLE PRECISION NOT NULL DEFAULT 0.00,
DROP COLUMN "streak_maintain",
ADD COLUMN     "streak_maintain" BOOLEAN NOT NULL DEFAULT false,
DROP COLUMN "last_streak_date",
ADD COLUMN     "last_streak_date" TIMESTAMP(3) NOT NULL;
