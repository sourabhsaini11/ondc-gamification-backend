/*
  Warnings:

  - You are about to drop the column `age` on the `User` table. All the data in the column will be lost.
  - You are about to drop the column `email` on the `User` table. All the data in the column will be lost.
  - A unique constraint covering the columns `[orderId]` on the table `User` will be added. If there are existing duplicate values, this will fail.

*/
-- DropIndex
DROP INDEX "User_email_key";

-- AlterTable
ALTER TABLE "User" DROP COLUMN "age",
DROP COLUMN "email",
ADD COLUMN     "basePrice" TEXT NOT NULL DEFAULT '0',
ADD COLUMN     "buyerAppId" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "category" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "convenienceFee" TEXT NOT NULL DEFAULT '0',
ADD COLUMN     "discount" TEXT NOT NULL DEFAULT '0',
ADD COLUMN     "orderId" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "orderStatus" TEXT NOT NULL DEFAULT 'Pending',
ADD COLUMN     "sellerId" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "shippingCharges" TEXT NOT NULL DEFAULT '0',
ADD COLUMN     "taxes" TEXT NOT NULL DEFAULT '0',
ADD COLUMN     "timestampCreated" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "timestampUpdated" TEXT NOT NULL DEFAULT '',
ADD COLUMN     "uid" TEXT NOT NULL DEFAULT '';

-- CreateIndex
CREATE UNIQUE INDEX "User_orderId_key" ON "User"("orderId");
