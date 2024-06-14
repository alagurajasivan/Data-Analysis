
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100)
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    order_price DECIMAL(10, 2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

INSERT INTO customers (customer_id, customer_name) VALUES
(1, 'John Doe'),
(2, 'Jane Smith'),
(3, 'Sam Johnson');

INSERT INTO orders (order_id, customer_id, order_date, order_price) VALUES
(1, 1, '2013-06-15', 250),
(2, 1, '2015-07-19', 300),
(3, 2, '2014-09-05', 450),
(4, 3, '2016-10-21', 500),
(5, 1, '2017-01-12', 700),
(6, 2, '2018-03-30', 200),
(7, 3, '2020-11-11', 300);

WITH FirstOrderDate AS (
    SELECT MIN(order_date) AS first_order_date
    FROM orders
),
OrdersWithin10Years AS (
    SELECT o.order_id, o.customer_id, o.order_date, o.order_price, f.first_order_date
    FROM orders o
    CROSS JOIN FirstOrderDate f
    WHERE o.order_date <= DATE_ADD(f.first_order_date, INTERVAL 10 YEAR)
),
MaxOrder AS (
    SELECT customer_id, MAX(order_price) AS max_order_price
    FROM OrdersWithin10Years
    GROUP BY customer_id
)
SELECT c.customer_name, o.order_price
FROM customers c
JOIN MaxOrder m ON c.customer_id = m.customer_id
JOIN orders o ON o.customer_id = m.customer_id AND o.order_price = m.max_order_price;



