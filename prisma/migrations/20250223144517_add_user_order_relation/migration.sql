/*
  Warnings:

  - You are about to drop the column `userId` on the `orderData` table. All the data in the column will be lost.
  - Added the required column `uploaded_by` to the `orderData` table without a default value. This is not possible if the table is not empty.

*/
-- DropForeignKey
ALTER TABLE "orderData" DROP CONSTRAINT "orderData_userId_fkey";

-- AlterTable
ALTER TABLE "orderData" DROP COLUMN "userId",
ADD COLUMN     "uploaded_by" INTEGER NOT NULL;
