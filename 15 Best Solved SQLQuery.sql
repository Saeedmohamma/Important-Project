
---Answered - 1--




with new_present as (
select *, ROW_NUMBER() over(order by employee) as number_1
from emp_attendance
),

new_present2 as (
select EMPLOYEE, DATES, STATUS, number_1 - ROW_NUMBER() over(order by employee) as part_number
from new_present
where status = 'present'
),

new_absent2 as (
select EMPLOYEE, DATES, STATUS, number_1 - ROW_NUMBER() over(order by employee) as absent_number from new_present
where status = 'absent'
)

select distinct employee, min(dates) over(partition by part_number, employee order by employee, part_number) as FROM_DATE
, max(dates) over(partition by part_number, employee order by employee, part_number) as TO_DATE
, status
from new_present2

union

select distinct employee, min(dates) over(partition by absent_number, employee order by employee, absent_number) as FROM_DATE
, max(dates) over(partition by absent_number, employee order by employee, absent_number) as TO_DATE
, status 
from new_absent2
order by employee


------------------------------------x----------------------------------------x----------------------------------------------------------

---Answered - 2--

select id, case when p_id is null then 'root'
           when p_id is not null and id in (select distinct p_id from tree) then 'inner'
   else 'leaf'
   end as type
 from tree


 -------------------------------x------------------------------------x-----------------------------------------------------------

 ---Answered - 3--


select year, brand, amount
from
(
select *, count(in_dec_num) over(partition by brand order by year, brand 
      range between unbounded preceding and unbounded following) as num_count
from
(
select *,  case when amount >lag(amount,1,0) over(partition by brand order by year, brand)
          then 1 else 0 end as  in_dec_num
 
from brands
)x
where in_dec_num = 1
)y
where num_count > 2


----------------------x--------------------------------------------x---------------------------x---------------

------Answered - 4-----------------


with credit_dbit as (
      select *,  sum(transaction_amount) over(partition by account_no, debit_credit 
             order by account_no, debit_credit rows unbounded preceding) as sum_number
              from account_balance
   ),

   final_table as (
   select cd1.account_no, transaction_date, debit_credit, 
         case when debit2.sum_number >0 then cd1.sum_number - debit2.sum_number 
 else cd1.sum_number end as actual_number
   from credit_dbit cd1
   left join (select account_no, cd2.sum_number from credit_dbit cd2 where cd2.debit_credit = 'debit') debit2
   on debit2.account_no = cd1.account_no 
   where cd1.sum_number >=1000 and debit_credit = 'credit'
   )

   select account_no, transaction_date
   from final_table
   where actual_number >=1000



----------------------x--------------------------------------------x---------------------------x---------------

------Answered - 5-----------------

with table1 as (
select distinct name 
from Q4_data
where name is not null
),

table2 as (
select distinct location
from Q4_data
where location is not null
)

select 1 as id, name, location
from table1 t1, table2 t2



----------------------x--------------------------------------------x---------------------------x---------------

------Answered - 6-----------------

--1nd soluation---

select test_id, marks 
from (select *, lag(marks,1,0) over(order by test_id) as prev_test_mark
from student_tests) x
where x.marks > prev_test_mark;



---2nd soluation-----

select *
from (select *, lag(marks,1,marks) over(order by test_id) as prev_test_mark
from student_tests) x
where x.marks > prev_test_mark;



----------------------x--------------------------------------------x---------------------------x---------------

------Answered - 7-----------------


with orders_t1 as 
(
select CUSTOMER_ID, DATES,  STRING_AGG(cast(product_id as varchar), ', ')  as merge_product_id
from orders
group by CUSTOMER_ID, DATES
)

select dates, cast(PRODUCT_ID as varchar) as PRODUCT_ID
from orders
union
select dates,  cast(merge_product_id as varchar)  as PRODUCT_ID
from orders_t1
order by dates, PRODUCT_ID



----------------------x--------------------------------------------x---------------------------x---------------

------Answered - 8-----------------


select  emp.name, count(man.manager) as NO_OF_EMPLOYEES
from employee_managers emp, employee_managers Man
where emp.id = man.manager
group by emp.name
order by NO_OF_EMPLOYEES desc


----------------------x--------------------------------------------x---------------------------x---------------

------Answered - 9-----------------

select store_id, sum(case when product_new like 'APPLE%' Then 1 else 0 end) as count_num,
sum(case when product_new2 like 'APPLE%' Then 1 else 0 end) as count_numer2
from
(
select store_id,  trim(upper(product_1)) as product_new, trim(upper(product_2)) as product_new2 
from product_demo
)x
group by store_id



----------------------x--------------------------------------------x---------------------------x---------------

------Answered - 10-----------------

select emp_name, Basic, Allowance, Others, (Basic+ Allowance +Others) as GROSS, INSURANCE, HEALTH, HOUSE 
, (INSURANCE+ HEALTH + HOUSE) as TOTAL_DEDUCTIONS
, (Basic+ Allowance +Others) - (INSURANCE+ HEALTH + HOUSE) as NET_PAY 
from

(

------1st SOLUATION-----

select s1.emp_id, EMP_NAME, DEDUCTION as TRNS_TYPE, (base_salary*de.percentage)/100 as amount 
from salary s1
cross join deduction de

union all

select s1.emp_id, EMP_NAME, income as TRNS_TYPE, (base_salary*I.percentage)/100 as amount 
from salary s1
cross join income I

-----END HERE 1ST SOUATION

) salary_Statment

pivot
(
sum([amount])
for [TRNS_TYPE]
in ([BASIC],[ALLOWANCE], [OTHERS], [INSURANCE], [HEALTH], [HOUSE])
)
as pivottable



----------------------x--------------------------------------------x---------------------------x---------------

------Answered - 11-----------------


with cte_1 as (
select *,
day(login_date) - DENSE_RANK() over(partition by user_id order by user_id, LOGIN_DATE) as difference_no
from user_login
group by user_id, login_date
),

cte_2 as (
select  user_id, login_date,
min(login_Date) over(partition by user_id, difference_no order by user_id) as start_Date1,
max(login_Date) over(partition by user_id, difference_no order by user_id) as end_Date,
difference_no
from cte_1
)

select  user_id, start_Date1, end_Date, count(difference_no) as max_date  
from cte_2
group by user_id, start_Date1, end_Date
having count(difference_no) >=5



----------------------x--------------------------------------------x---------------------------x---------------

------Answered - 12-----------------


select * from employees
select * from events

select e.name, count(distinct event_name) NO_OF_EVENTS
from employees e
inner join events ev on e.id = ev.emp_id
group by e.name
having count(distinct event_name) = (select count(distinct event_name) from events) 



----------------------x--------------------------------------------x---------------------------x---------------

------Answered - 13-----------------


with middle_number as (
select *, 
ROW_NUMBER() over(partition by country order by id ) as max_count,
cast(count(country) over(partition by country order by id range between unbounded preceding 
and unbounded following) as float) as max_number
from people
)

select country, age
from middle_number
where max_count>=(max_number/2) and max_count <=(max_number/2)+1



----------------------x--------------------------------------------x---------------------------x---------------

------Answered - 14-----------------


select post_id, sum(total_Seconds) as total_view_time
from 
(
select us.session_id, DATEDIFF(second, session_starttime, session_endtime)*perc_viewed/100 as total_Seconds,
pv.session_id as post_sesson_id,pv.post_id,pv.perc_viewed
from user_sessions us
inner join post_views pv on us.session_id = pv.session_id
)x
group by post_id
having sum(total_Seconds) >5
order by total_view_time



----------------------x--------------------------------------------x---------------------------x---------------

------Answered - 15-----------------


select id,  string_agg(len(value), ',') as number_items from
(
select *
from item
cross apply string_split(items, ',')
)x
group by id, items
order by id


