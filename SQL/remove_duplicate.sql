-- give table below remove duplicate rows from the table

-- custid	orderid	    amount
-- 1	        o1	    10
-- 1	        o2	    45
-- 2	        o3	    30
-- 2	        o4	    23
-- 2	        o4      23
-- 3	        o6	    45
-- 3	        o7	    90


WITH CTE AS (
    SELECT 


    
        custid,
        orderid,
        amount,
        ROW_NUMBER() OVER(PARTITION BY custid, orderid, amount ORDER BY (SELECT NULL)) as row_num
    FROM transcation
)
DELETE FROM CTE WHERE row_num > 1;