/*
  Warnings:

  - You are about to drop the column `gmv` on the `orderData` table. All the data in the column will be lost.
  - You are about to drop the column `points` on the `orderData` table. All the data in the column will be lost.

*/
-- DropIndex
DROP INDEX "orderData_order_id_idx";

-- AlterTable
ALTER TABLE "orderData" DROP COLUMN "gmv",
DROP COLUMN "points";

-- CreateIndex
CREATE INDEX "orderData_order_id_buyer_app_id_order_status_idx" ON "orderData"("order_id", "buyer_app_id", "order_status");
