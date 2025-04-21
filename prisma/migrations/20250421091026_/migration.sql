/*
  Warnings:

  - You are about to drop the column `updated_by_lambda` on the `orderData` table. All the data in the column will be lost.
  - You are about to drop the column `uploaded_by` on the `orderData` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "orderData" DROP COLUMN "updated_by_lambda",
DROP COLUMN "uploaded_by";
