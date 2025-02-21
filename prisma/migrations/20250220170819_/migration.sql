/*
  Warnings:

  - You are about to drop the column `gameId` on the `User` table. All the data in the column will be lost.

*/
-- DropIndex
DROP INDEX "User_gameId_key";

-- DropIndex
DROP INDEX "User_orderId_key";

-- AlterTable
ALTER TABLE "User" DROP COLUMN "gameId";
