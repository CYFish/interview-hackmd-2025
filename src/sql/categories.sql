CREATE TABLE categories (
    category_id VARCHAR(50) PRIMARY KEY,
    category_name VARCHAR(256),
    parent_category_id VARCHAR(50) REFERENCES categories(category_id)
);