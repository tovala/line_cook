CREATE TABLE brine.experian_customers_temp AS
SELECT 
    COALESCE(c.first_name, '') || '|' || 
    COALESCE(c.last_name, '') || '|' || 
    COALESCE(a.line_1, '') || '|' || 
    COALESCE(a.line_2, '') || '|' || 
    COALESCE(a.city, '') || '|' || 
    COALESCE(a.state, '') || '|' || 
    COALESCE(a.zip_cd, '') || '|' || 
    COALESCE(c.email, '') || '|' ||
    COALESCE(a.phone_number, '') || '|' || 
    c.customer_id::STRING AS request_body
    , ROW_NUMBER() OVER (ORDER BY c.customer_id) AS row_number
FROM grind.customers c
LEFT JOIN grind.addresses a 
ON c.customer_id = a.customer_id 
WHERE c.customer_id NOT IN 
(SELECT customer_id 
FROM wash.experian_responses
WHERE customer_id IS NOT NULL);