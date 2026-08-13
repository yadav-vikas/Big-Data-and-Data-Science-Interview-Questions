-- transaction table:	 
-- custid	orderid	amount
-- 1	o1	10
-- 1	o2	45
-- 2	o3	30
-- 2	o4	23
-- 2	o5	56
-- 3	o6	45
-- 3	o7	90

-- expected output:	 
-- custid	total amount	orderid
-- 1	        55	        o1|o2
-- 2	        109	        o3|o4|o5
-- 3	        135	        o6|o7

SELECT custid,
        SUM(amount) AS total_amount,
        STRING_AGG(orderid, "|" ORDER BY orderid) as order_id
    FROM transcation
    GROUP BY custid

-- T-SQL
-- SELECT 
--     custid,
--     SUM(amount) AS total_amount,
--     STRING_AGG(orderid, '|') WITHIN GROUP (ORDER BY orderid) AS order_id
-- FROM transcation
-- GROUP BY custid;