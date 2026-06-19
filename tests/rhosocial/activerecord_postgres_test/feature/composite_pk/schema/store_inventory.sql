CREATE TABLE "store_inventory" (
    "store_id" INTEGER NOT NULL,
    "product_id" INTEGER NOT NULL,
    "batch_id" VARCHAR(64) NOT NULL,
    "stock" INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY ("store_id", "product_id", "batch_id")
);
