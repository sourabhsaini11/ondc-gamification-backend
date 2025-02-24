/*
  Warnings:

  - Added the required column `updated_by_lambda` to the `OrderData` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "OrderData" ADD COLUMN     "updated_by_lambda" TIMESTAMP(3) NOT NULL;
