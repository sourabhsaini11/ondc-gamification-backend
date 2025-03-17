-- CreateTable
CREATE TABLE "rewardledger" (
    "id" SERIAL NOT NULL,
    "order_id" TEXT NOT NULL DEFAULT '',
    "game_id" TEXT NOT NULL DEFAULT '',
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "points" DOUBLE PRECISION NOT NULL DEFAULT 0.00,
    "reason" TEXT NOT NULL DEFAULT '',

    CONSTRAINT "rewardledger_pkey" PRIMARY KEY ("id")
);
