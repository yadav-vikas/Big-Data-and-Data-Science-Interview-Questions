-- STRING MANIPULATION IN SQL

-- 1. combining string
SELECT CONCAT('streat dublin', '55', 'Germany') as location;

-- 2. combining strings with seperator
SELECT CONCAT_WS(',', 'Street ROMA', '72', 'Dublin') as full_address;

-- 3. upper, lower trim 
SELECT LOWER('Accounting') as dept, Upper('management') as sector;
SELECT TRIM(' Street ROMA, 72 ') as trimmed_address;
SELECT LTRIM(' Street ROMA, 72 ') as ltrimmed_address;
SELECT RTRIM(' Street ROMA, 72 ') as rtrimmed_address;
-- SELECT TRIM('+++345', '+') as trimm_string;

-- 4. substring
SELECT  
    SUBSTRING('Antony', 1, 1) as first_character,
    SUBSTRING('Antory', 1, 3) as first_3_charater,
    SUBSTRING('Antony', 1, 5) as first_5_character;

-- 5. LEN with CONCAT
SELECT
LEN('antony') AS lenght_name,
SUBSTRING(UPPER('antony'),1,1) AS first_character,
SUBSTRING(LOWER('antony'),2,LEN('antony')) AS  last_characters,
CONCAT(SUBSTRING(UPPER('antony'),1,1),SUBSTRING(LOWER('antony'),
      2,LEN('antony'))) AS name; 

-- 6.repalce string
SELECT 'street ROMA - 32' as address,
    replace('street ROMA - 32', '-', ',') as cleaned_address;

-- 7.charindex
SELECT 'street ROMA = 32' as address,
    LEN('street ROMA - 32') as lenght_address,
    CHARINDEX('-', 'Street ROMA - 32') as location_dash;

-- 8.reverse string
SELECT REVERSE("abc") as reverse_string;

-- 9. filtering based on string match (last name start with 's')
SELECT * FROM employees WHERE first_name LEFT(last_name,1) = 'S';


-- 10.String aggregation
SELECT STRING_AGG(first_name, '|') as name_with_pipe;

