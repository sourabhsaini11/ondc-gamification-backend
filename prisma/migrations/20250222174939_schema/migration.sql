/*
  Warnings:

  - A unique constraint covering the columns `[game_id]` on the table `leaderboard` will be added. If there are existing duplicate values, this will fail.

*/
-- CreateIndex
CREATE UNIQUE INDEX "leaderboard_game_id_key" ON "leaderboard"("game_id");
