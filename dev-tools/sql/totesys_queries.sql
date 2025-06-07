-- retrieve records from totesys

-- Query: View all rows from a table
-- Whitelisted tables:
        -- "counterparty"
        -- "currency"
        -- "department"
        -- "design"
        -- "staff"
        -- "sales_order"
        -- "address"
        -- "payment"
        -- "purchase_order"
        -- "payment_type"
        -- "transaction"
SELECT * FROM staff;
SELECT * FROM payment;

-- Query: Count number of records in each table
SELECT COUNT(*) FROM department;
SELECT COUNT(*) FROM purchase_order;

-- Query: Staff working in a department
SELECT s.staff_id, s.first_name, s.last_name, d.department_name
FROM staff s
JOIN department d ON s.department_id = d.department_id;

-- Query: Total payments by type
-- SELECT pt.payment_type_name, SUM(p.amount) AS total_paid
-- FROM payment p
-- JOIN payment_type pt ON p.payment_type_id = pt.payment_type_id
-- GROUP BY pt.payment_type_name;

-- Query: Orders placed in the last 30 days
-- SELECT *
-- FROM purchase_order
-- WHERE created_at >= CURRENT_DATE - INTERVAL '30 days';

-- Get records updated after a certain timestamp -- This is useful for ingestion
SELECT * FROM transaction
WHERE last_updated > '2025-05-01 00:00:00';