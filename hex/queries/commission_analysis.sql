-- SQL Template for Commission Analysis in Hex
-- Use this query in a Hex SQL cell.
-- Assumes Hex Input Parameters named 'date_range' (Date range type) and 'department' (Dropdown/Multiselect type) exist.
-- Assumes tables are in finance_dw.facts and finance_dw.dimensions catalogs.
-- Adjust column names (e.g., e.employee_name, e.role, d.year, d.month) based on final dimension table definitions.

SELECT
  e.employee_name, -- Adjust based on dim_employee
  e.role, -- Adjust based on dim_employee
  d.year, -- Adjust based on dim_date
  d.month, -- Adjust based on dim_date
  COUNT(f.deal_id) AS deals_closed,
  SUM(f.setter_commission) AS setter_commission,
  SUM(f.closer_commission) AS closer_commission,
  SUM(f.profit) AS profit_generated
FROM finance_dw.facts.fact_deals f
JOIN finance_dw.dimensions.dim_employee e ON f.employee_key = e.employee_key -- Assuming employee_key links deals to employees
JOIN finance_dw.dimensions.dim_date d ON f.completion_date_key = d.date_key -- Using completion date for commission
WHERE d.date BETWEEN '{{ date_range.start }}' AND '{{ date_range.end }}'
  AND e.department = '{{ department }}' -- Assumes department parameter provides a single value for equality check
GROUP BY e.employee_name, e.role, d.year, d.month
ORDER BY SUM(f.profit) DESC; 