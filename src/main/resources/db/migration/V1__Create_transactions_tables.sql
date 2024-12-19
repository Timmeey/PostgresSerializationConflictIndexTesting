CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    transaction_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    customer_name VARCHAR(100) NOT NULL
);

CREATE TABLE transaction_details (
    id SERIAL PRIMARY KEY,
    transaction_id INTEGER NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES transactions(id)
);