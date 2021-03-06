-- =================================
-- GOOD BIGQUERY SQL TIPS TO KNOW --
-- =================================

COUNT(DISTINCT CASE WHEN x THEN y END) -- COUNT (DISTINCT) ACCORDING TO A CONDITION --

-- DATES --
DATE(TIMESTAMP(x)) -- CONVERT STRING TO TIMESTAMP AND THEN TO DATE
DATE_TRUNC(x, MONTH) -- GET THE FIRST DAY OF DAY - FORMAT : 2020-01-01
DATE_DIFF(latest, soonest, DAY/MONTH/YEAR) -- COMPUTE DIFFERENCE IN DAY/MONTH/YEAR BETWEEN TWO DATE
TIMESTAMP_DIFF(latest, soonest, DAY/MONTH/YEAR) -- COMPUTE DIFFERENCE IN DAY/MONTH/YEAR BETWEEN TWO TIMESATMP

-- USEFUL --
HAVING AGG_FUNCTION() -- USEFUL TO FILTER ON A AGGREGATE FUNCTION - REPLACE AGG() BY SUM(), COUNT(), AVG() ETC...
COALESCE(value_1, value_2) -- IF THE VALUE_1 IS NULL THEN THE VALUE 2 WILL BE USED

-- WINDOW FUNCTIONS --
SUM() OVER (PARTITION BY x)
SUM() OVER (PARTITION BY x ORDER BY ASC)
FIRST_VALUE() OVER (ORDER BY x DESC)
RANK() OVER (PARTITION BY x ORDER BY x ASC)

-- HELPFUL --
SAFE.PARSE_DATE('%Y-%m-%d', x) -- CONVERT DATE STRING FORMAT TO DATE - POSSIBLE TO SPECIFY THE EXACT FORMAT
CAST(x AS STRING) -- CAST A VALUE TO ANY TYPE - TYPE : INT64, FLOAT64, STRING ETC...
ROUND(x, decimal) -- ROUND A INTEGER OR FLOAT VALUE TO DECIMAL PARAMATER
SAVE_DIVIDE() -- AVOID DIVIDING BY ZERO AND HAVE AN ERROR - USE SAFE_DIVIDE INSTEAD

-- ADVANCED --
UNNEST() -- ALLOW TO UNNEST AN ARRAY - CREATE AS MANY LINES AS VALUE IN THE ARRAY
GENERATE_ARRAY() -- ALLOW TO GENERATE ARRAY OF INT64 OR FLOAT64 BETWEEN A RANGE
GENERATE_DATE_ARRAY() -- ALLOW TO GENERATE A DATE ARRAY BETWEEN A RANGE
GENERATE_TIMESTAMP_ARRAY() -- ALLOW TO GENERATE A TIMESTAMP ARRAY BETWEEN A RANGE

-- ==================================
-- BITS OF CODE --
-- ==================================

-- ACCOUNT  MANAGERS --
WHERE shops.account_manager_email IN ('theo@selency.com', 'victoire@selency.com', 'philippine.m@selency.com')

-- DATE AGGREGATIONS --
created_at,
DATE_TRUNC(created_at, WEEK(SATURDAY)) AS start_week,
DATE_TRUNC(created_at, MONTH) AS start_month,

-- GENERATE AN ARRAY --
DECLARE numbers ARRAY<INT64>; -- DECLARE AN ARRAY VARIABLE OF TYPE INT64

SET numbers = ( -- FILL THE ARRAY OF VALUE BETWEEN RANGE [1-60]
  SELECT GENERATE_ARRAY(1, 60)
);

WITH query1 AS (
	XXX
), query 2 AS (
	XXX
)
SELECT
	days,
	COUNT(DISTINCT CASE WHEN diff <= days THEN x END)
FROM table
CROSS JOIN UNNEST(numbers) AS days
GROUP BY 1
ORDER BY 1
