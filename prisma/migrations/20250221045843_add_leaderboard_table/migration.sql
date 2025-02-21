/*
  Warnings:

  - You are about to drop the `User` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropTable
DROP TABLE "User";

-- CreateTable
CREATE TABLE "OrderData" (
    "id" SERIAL NOT NULL,
    "uid" TEXT NOT NULL DEFAULT '',
    "name" TEXT NOT NULL,
    "orderId" TEXT NOT NULL DEFAULT '',
    "orderStatus" TEXT NOT NULL DEFAULT 'Pending',
    "timestampCreated" TEXT NOT NULL DEFAULT '',
    "timestampUpdated" TEXT NOT NULL DEFAULT '',
    "category" TEXT NOT NULL DEFAULT '',
    "buyerAppId" TEXT NOT NULL DEFAULT '',
    "basePrice" TEXT NOT NULL DEFAULT '0',
    "shippingCharges" TEXT NOT NULL DEFAULT '0',
    "taxes" TEXT NOT NULL DEFAULT '0',
    "discount" TEXT NOT NULL DEFAULT '0',
    "convenienceFee" TEXT NOT NULL DEFAULT '0',
    "sellerId" TEXT NOT NULL DEFAULT '',

    CONSTRAINT "OrderData_pkey" PRIMARY KEY ("id")
);
