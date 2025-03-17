/*
  Warnings:

  - Added the required column `position` to the `dailywinner` table without a default value. This is not possible if the table is not empty.
  - Added the required column `type` to the `dailywinner` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "dailywinner" ADD COLUMN     "position" INTEGER NOT NULL,
ADD COLUMN     "type" TEXT NOT NULL;
