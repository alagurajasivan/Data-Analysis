CREATE TABLE memberships (
    membership_id INT,
    membership_month DATE,
    customer_id INT
);



INSERT INTO memberships (membership_id, membership_month, customer_id) VALUES
(1, '2023-01-01', 1),
(2, '2023-04-01', 1),
(3, '2023-07-01', 1),
(4, '2023-01-01', 2),
(5, '2023-02-01', 2),
(6, '2023-05-01', 2),
(7, '2023-01-01', 3),
(8, '2023-02-01', 3),
(9, '2023-03-01', 3),
(10, '2023-01-01', 4),
(11, '2023-03-01', 4);



WITH MembershipGaps AS (
    SELECT 
        m1.customer_id,
        m1.membership_month AS start_month,
        LEAD(m1.membership_month, 1) OVER (PARTITION BY m1.customer_id ORDER BY m1.membership_month) AS next_month
    FROM 
        memberships m1
),
GapCalculation AS (
    SELECT
        customer_id,
        start_month,
        next_month,
        TIMESTAMPDIFF(MONTH, start_month, next_month) AS gap_months
    FROM 
        MembershipGaps
)

select * from GapCalculation;


SELECT 
    DISTINCT customer_id
FROM 
    GapCalculation
WHERE 
    gap_months >= 2;
