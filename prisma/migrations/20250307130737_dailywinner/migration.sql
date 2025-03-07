-- AlterTable
ALTER TABLE "rewardledger" ADD COLUMN     "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP;

-- CreateTable
CREATE TABLE "dailywinner" (
    "id" SERIAL NOT NULL,
    "game_id" INTEGER NOT NULL,
    "points" INTEGER NOT NULL,
    "winning_date" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "dailywinner_pkey" PRIMARY KEY ("id")
);
