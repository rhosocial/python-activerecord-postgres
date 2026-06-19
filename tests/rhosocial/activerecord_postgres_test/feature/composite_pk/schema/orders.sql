CREATE TABLE "orders" (
    "id" SERIAL PRIMARY KEY,
    "total" DECIMAL(10,2) NOT NULL,
    "created_at" TIMESTAMP WITH TIME ZONE,
    "updated_at" TIMESTAMP WITH TIME ZONE
);
