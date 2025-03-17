/*
  Warnings:

  - You are about to drop the column `base_price` on the `orderData` table. All the data in the column will be lost.
  - You are about to drop the column `convenience_fee` on the `orderData` table. All the data in the column will be lost.
  - You are about to drop the column `discount` on the `orderData` table. All the data in the column will be lost.
  - You are about to drop the column `shipping_charges` on the `orderData` table. All the data in the column will be lost.
  - You are about to drop the column `taxes` on the `orderData` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "orderData" DROP COLUMN "base_price",
DROP COLUMN "convenience_fee",
DROP COLUMN "discount",
DROP COLUMN "shipping_charges",
DROP COLUMN "taxes",
ADD COLUMN     "total_price" DOUBLE PRECISION NOT NULL DEFAULT 0.00;
