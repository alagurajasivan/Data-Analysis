CREATE TABLE orders_1 (
    order_date DATE,
    order_line_id INT,
    item_id INT,
    customer_id INT,
    order_amount DECIMAL(10, 2)
);


INSERT INTO orders_1 (order_date, order_line_id, item_id, customer_id, order_amount) VALUES
('2024-01-05', 1, 101, 1001, 50.00),
('2024-01-05', 2, 102, 1002, 75.00),
('2024-01-15', 3, 103, 1001, 25.00),
('2024-02-10', 4, 101, 1003, 40.00),
('2024-02-20', 5, 102, 1003, 35.00),
('2024-03-05', 6, 101, 1002, 20.00),
('2024-03-10', 7, 103, 1002, 50.00),
('2024-03-15', 8, 104, 1004, 30.00),
('2024-04-10', 9, 105, 1004, 60.00),
('2024-04-15', 10, 106, 1005, 90.00);


select * from orders_1;

SELECT customer_id,count(item_id) as co,year(order_date),MONTH(order_date)
FROM orders_1
GROUP BY customer_id, YEAR(order_date), MONTH(order_date)
HAVING COUNT(DISTINCT item_id) > 1;



