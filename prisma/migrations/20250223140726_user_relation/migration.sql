/*
  Warnings:

  - Added the required column `userId` to the `orderData` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "orderData" ADD COLUMN     "userId" INTEGER NOT NULL;

-- AddForeignKey
ALTER TABLE "orderData" ADD CONSTRAINT "orderData_userId_fkey" FOREIGN KEY ("userId") REFERENCES "User"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
