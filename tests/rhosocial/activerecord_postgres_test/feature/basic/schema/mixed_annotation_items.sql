CREATE TABLE "mixed_annotation_items" (
    "id" SERIAL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "tags" TEXT,
    "meta" TEXT,
    "description" TEXT,
    "status" TEXT
);
