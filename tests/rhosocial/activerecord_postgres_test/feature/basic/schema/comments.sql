CREATE TABLE "comments" (
    "id" SERIAL PRIMARY KEY,
    "post_ref" INT NOT NULL,
    "author" INT NOT NULL,
    "text" TEXT NOT NULL,
    "created_at" TIMESTAMP NOT NULL,
    "updated_at" TIMESTAMP,
    "approved" BOOLEAN DEFAULT false,
    FOREIGN KEY ("post_ref") REFERENCES "posts"("id") ON DELETE CASCADE,
    FOREIGN KEY ("author") REFERENCES "users"("id") ON DELETE CASCADE
);
