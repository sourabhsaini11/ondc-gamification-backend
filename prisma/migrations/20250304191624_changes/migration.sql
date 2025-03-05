/*
  Warnings:

  - You are about to drop the column `category` on the `orderData` table. All the data in the column will be lost.
  - You are about to drop the column `seller_id` on the `orderData` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "orderData" DROP COLUMN "category",
DROP COLUMN "seller_id",
ADD COLUMN     "domain" TEXT NOT NULL DEFAULT '';
