CREATE TABLE "posts" (
    "id" SERIAL PRIMARY KEY,
    "author" INT NOT NULL,
    "title" VARCHAR(255) NOT NULL,
    "content" TEXT,
    "published_at" TIMESTAMP,
    "published" BOOLEAN DEFAULT false,
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP,
    FOREIGN KEY ("author") REFERENCES "users"("id") ON DELETE CASCADE
);
