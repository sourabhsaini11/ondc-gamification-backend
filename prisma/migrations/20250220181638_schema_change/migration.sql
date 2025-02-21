-- CreateTable
CREATE TABLE "Leaderboard" (
    "id" SERIAL NOT NULL,
    "uid" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "basePoints" INTEGER NOT NULL,
    "gmvPoints" INTEGER NOT NULL,
    "highValueBonus" INTEGER NOT NULL,
    "totalPoints" INTEGER NOT NULL,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Leaderboard_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Leaderboard_uid_key" ON "Leaderboard"("uid");
