Note: For Code Optimization, We created HRV column in IBI table using temp table. It stores the HRV value for all the patient. we haven’t calculated HRV separately for the queries needed instead we used it from IBI table.

*********************created the hrv column in IBI Table*************
-- Create a temporary table to store HRV calculations
CREATE TEMP TABLE Temp_HRV AS
SELECT
    patientid,
    datestamp,
    rmssd_ms,
    AVG(rmssd_ms) OVER (PARTITION BY patientid ORDER BY datestamp ASC) * 600 AS HRV 
FROM 
	ibi;

--altering the IBI table with adding HRV column
ALTER TABLE IBI
ADD COLUMN HRV numeric;


-- Update the IBI table with HRV values
UPDATE    ibi SET     HRV = Temp_HRV.HRV
FROM     Temp_HRV
WHERE
    ibi.patientid = Temp_HRV.patientid
    AND ibi.datestamp = Temp_HRV.datestamp;	

-- Drop the temporary table
DROP TABLE Temp_HRV;

-- selecting the table 
select patientid, AVG(rmssd_ms)*600 as hrv from ibi group by patientid
******************************************************************************

@@@ One Other Team Member created the HRV table Seaprately  and she used this

DROP TABLE If EXISTS HRV
CREATE TABLE HRV AS
SELECT
    patientid,
    datestamp,
    rmssd_ms,
    AVG(rmssd_ms) OVER (PARTITION BY patientid ORDER BY datestamp ASC) * 600 AS HRV FROM ibi;








--Q1: Write a query to get a list of patients with event type of EGV and glucose (mgdl)greater than 155 .
--Query
SELECT id, event_type, glucose_value_mgdl
FROM public.eventtype WHERE event_type = 'EGV' AND glucose_value_mgdl > 155;


--Q2 :How many patients consumed meals with at least 20 grams of protein in it?
--Query
SELECT COUNT(DISTINCT patientid) as PatientCount
FROM foodlog
WHERE protein >= 20;


--Q3 Who consumed maximum calories during dinner? (assuming the dinner time is after 6pm-8pm)
--Query
WITH total_dinner_calories as(
SELECT patientid,
	   sum(calorie) as total_dinner_calorie
FROM
	foodlog
WHERE 
	EXTRACT(HOUR FROM datetime) >= 18 AND EXTRACT(HOUR FROM datetime) < 20
GROUP BY patientid
)

SELECT t.patientid,
       t.total_dinner_calorie As Maximum_Dinner_Calorie
FROM total_dinner_calories t
WHERE t.total_dinner_calorie=(SELECT MAX(x.total_dinner_calorie) from total_dinner_calories x)


--Q4: Which patient showed a high level of stress on most days recorded for him/her?
--Query
WITH StressCounts AS ( SELECT
    e.patientid,
    COUNT(*) AS high_stress_days
  FROM   eda AS e   WHERE
    e.max_eda > 40
  GROUP BY
    e.patientid
)
SELECT   s.patientid FROM   StressCounts s
WHERE
  s.high_stress_days = (
    SELECT MAX(high_stress_days) FROM StressCounts
  );



--Q5: Based on mean HR and HRV alone, which patient would be considered least healthy?
--Query
SELECT   i.patientid,
  (AVG(h.max_hr) * AVG(i.hrv)) AS health_score
FROM   ibi AS i
JOIN   hr AS h ON i.patientid = h.patientid
GROUP BY   i.patientid
ORDER BY   health_score ASC
LIMIT 1;

--Q6 Create a table that stores any Patient Demographics as firstname, dob,patientid,hba1c of your choice as the parent table.
--Create a child table that contains max_EDA from EDA table  and mean_HR from HR  per patient and inherits all columns from the parent table 
--Query
drop table if exists patient_demographic cascade;

CREATE TABLE patient_demographic (
    patientid serial PRIMARY KEY,
    firstname VARCHAR(30) ,
    dob DATE ,
    hba1c DECIMAL(5, 2) 
);

INSERT INTO patient_demographic (firstname, dob, hba1c)
VALUES ('Jane', '1990-01-01', 5.5);

select * from patient_demographic;

drop table if exists patient_additional_info cascade

CREATE TABLE patient_additional_info (   
    max_eda DECIMAL(5, 2),
    mean_hr DECIMAL(5, 2), 
    patientid INT REFERENCES patient_demographic (patientid)
)INHERITS (patient_demographic);


INSERT INTO patient_additional_info(firstname, dob, hba1c,max_eda,mean_hr,patientid)
VALUES ('sita','1990-01-01',5.5,5.50,13.67,1); 


select * from patient_additional_info ;


--Q7 What percentage of the dataset is male vs what percentage is female?
--Query  
SELECT   gender,
  COUNT(*) AS count,
  Round((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM demographics)),2) AS percentage
FROM   demographics
GROUP BY   gender;


--Q8 Which patient has the highest max eda? 
--Query
With Max_eda_score as
(SELECT   patientid,
  MAX(max_eda) AS highest_max_eda
FROM   eda
GROUP BY   patientid
)					  
SELECT m.patientid,
	   m.highest_max_eda
FROM Max_eda_score m
WHERE 
m.highest_max_eda=(SELECT Max(x.highest_max_eda) from  Max_eda_score x)

--Q9: Display details of the prediabetic patients.
--Query
SELECT   * FROM demographics WHERE   hba1c >= 5.7 AND hba1c <= 6.4;


--Q10: List the patients that fall into the highest EDA category by name, gender and age
SELECT
  pd.firstname,
  pd.lastname,
  pd.gender,
  pd.dob
FROM
  demographics AS pd
INNER JOIN (  SELECT  patientid,     MAX(max_eda) AS highest_eda
  FROM     eda   GROUP BY     patientid
) AS max_eda_table ON pd.patientid = max_eda_table.patientid
WHERE   max_eda_table.highest_eda = (SELECT MAX(max_eda) FROM eda);

--Q11: How many patients have names starting with 'A'?
--Query
SELECT COUNT(*) as NumberofPatients FROM demographics
WHERE firstname LIKE 'A%';


--Q12: Show the distribution of patients across age.
--Query
SELECT   CASE     WHEN dob IS NOT NULL THEN 
      EXTRACT(YEAR FROM AGE(NOW(), dob))
    ELSE
      NULL
  END AS age,
  COUNT(*) AS patient_count
FROM   demographics GROUP BY   age ORDER BY   age;


--Q13: Display the Date and Time in 2 seperate columns for the patient who consumed only Egg
--Query
SELECT
  datetime AS consumption_datetime,
  CAST(datetime AS DATE) AS consumption_date,
  CAST(datetime AS TIME) AS consumption_time,
  logged_food
FROM
  foodlog
WHERE
  logged_food = 'Egg';


--Q14: Display list of patients along with the gender and hba1c for whom the glucose value is null.
--Query
SELECT
  d.patientid,
  d.gender,
  d.hba1c
FROM   demographics AS d
LEFT JOIN   dexcom AS g ON d.patientid = g.patientid
WHERE   g.glucose_value_mgdl IS NULL;


--Q15: Rank patients in descending order of Max blood glucose value per day
--Query
WITH MaxGlucosePerDay AS (
  SELECT
    patientid,
    DATE(datestamp) AS observation_date,
    MAX(glucose_value_mgdl) AS max_daily_glucose
  FROM     dexcom 
  GROUP BY
    patientid,
    DATE(datestamp)
)
SELECT
  patientid,
  observation_date,
  max_daily_glucose,
  RANK() OVER (PARTITION BY observation_date ORDER BY max_daily_glucose DESC) AS rank_per_day
FROM   MaxGlucosePerDay ORDER BY   observation_date DESC, max_daily_glucose DESC;




--Q16 : Assuming the IBI per patient is for every 10 milliseconds, calculate Patient-wise HRV from RMSSD.
/* to calculate HRV and create a column with HRV=Avg rmssd_ms from IBI per patientid from IBI * 600 */
--Query
-- Create a temporary table to store HRV calculations
CREATE TEMP TABLE Temp_HRV AS
SELECT
    patientid,
    datestamp,
    rmssd_ms,
    AVG(rmssd_ms) OVER (PARTITION BY patientid ORDER BY datestamp ASC) * 600 AS HRV FROM ibi;
	
ALTER TABLE IBI
ADD COLUMN HRV numeric;

-- Update the IBI table with HRV values
UPDATE    ibi SET     HRV = Temp_HRV.HRV
FROM     Temp_HRV
WHERE
    ibi.patientid = Temp_HRV.patientid
    AND ibi.datestamp = Temp_HRV.datestamp;

-- Drop the temporary table
DROP TABLE Temp_HRV;

--Q.17 What is the % of total daily calories consumed by patient 14 after 3pm Vs Before 3pm? 
--Query:	
 SELECT patientid,
 SUM(CASE WHEN  EXTRACT(hour FROM datetime) >= 15 THEN calorie ELSE 0 END) AS  calories_after_3pm,
 SUM(CASE WHEN  EXTRACT(hour FROM datetime) < 15 THEN calorie ELSE 0 END) AS calories_before_3pm,
 SUM(calorie) AS total_calories,
 (SUM(CASE WHEN EXTRACT(hour FROM datetime) >= 15 THEN calorie ELSE 0 END) * 100.0) / SUM(calorie) AS percentage_after_3pm,
 (SUM(CASE WHEN  EXTRACT(hour FROM datetime) < 15 THEN calorie ELSE 0 END) * 100.0) / SUM(calorie) AS percentage_before_3pm
FROM  foodlog
WHERE patientid = 14  
GROUP BY
    patientid;


--Q18 Display 5 random patients with HbA1c less than 6. 
--Query: 
SELECT CONCAT(firstname,' ',lastname)
AS patient_name FROM demographics
WHERE hba1c < 6
ORDER BY RANDOM() LIMIT 5;


--Q19 Generate a random series of data using any column from any table as the base.
--Query: 
SELECT calorie AS base_column, RANDOM() AS random_value
FROM foodlog; 
 
--Q20 Display the foods consumed by the youngest patient.
--Query: 
SELECT f.logged_food FROM foodlog f
INNER JOIN (SELECT patientid FROM demographics
            ORDER BY dob ASC
            LIMIT 1) AS d
            ON f.patientid =d.patientid;
 
--Q21 Identify the patients that has letter ‘h’ in their first name and print the last letter of their first name.
--Query:
SELECT firstname, RIGHT(firstname,1) AS lastletter
FROM demographics
WHERE firstname LIKE '%h%';
 
--Q22 Calculate the time spent by each patient outside the recommended blood glucose range.
--Query:
SELECT patientid,EXTRACT(EPOCH FROM (MAX(datestamp) - MIN(datestamp)))/3600 AS time_outside_range
FROM dexcom
WHERE
    (glucose_value_mgdl < 70 OR glucose_value_mgdl >= 126)
    GROUP BY patientid
    ORDER BY patientid;

--Q23 Show the time in minutes recorded by the Dexcom for every patient.
--Query:
SELECT patientid, EXTRACT(EPOCH FROM datestamp::TIME)/60 AS time
FROM dexcom; 

--Q24 List all the food eaten by patient Phill Collins.
--Query:
SELECT f.logged_food FROM foodlog f
INNER JOIN (SELECT patientid ,CONCAT(firstname,' ',lastname) AS patient_name FROM demographics) AS d
           ON f.patientid = d.patientid
          WHERE  d.patient_name= 'Phill Collins';
 
--Q25 Create a stored procedure to delete the min_EDA column in the table EDA.
--Query:
CREATE PROCEDURE delete_column()
LANGUAGE plpgsql
AS $$
BEGIN
  EXECUTE 'ALTER TABLE eda DROP COLUMN min_EDA';
    EXCEPTION
        WHEN others THEN
            RAISE NOTICE 'Column min_EDA does not exist in table EDA';
    END;
  $$
  
CALL delete_column();

SELECT * FROM eda; 


--Q26 When is the most common time of day for people to consume spinach?
--Query: 
SELECT datetime::TIME AS time,
COUNT(*) AS repeatingNO
FROM foodlog
WHERE logged_food = 'Spinach'
GROUP BY time
ORDER BY repeatingNo DESC
LIMIT 1; 

--Q27 Classify each patient based on their HRV range as high, low, or normal.
--Query:
SELECT patientid,(AVG(rmssd_ms)*600) AS hrv,
CASE
    WHEN (AVG(rmssd_ms) * 600) < 20 THEN 'Low'
    WHEN (AVG(rmssd_ms) * 600) >= 20 AND (AVG(rmssd_ms) * 600) <=30 THEN 'Normal'
    ELSE 'High'
    END
    FROM ibi  
    GROUP BY patientid
    ORDER BY patientid; 

--Q28 List full name of all patients with ‘an’ in their first or last names.
--Query:
SELECT CONCAT(firstname,' ',lastname) AS fullname FROM demographics
WHERE firstname LIKE '%an%' OR lastname LIKE '%an%'; 


--Q29 Display a pie chart of gender vs average HbA1c.
--Query:
SELECT gender, AVG(HbA1c)
FROM demographics
GROUP BY gender;

--30 The recommended daily allowance of fiber is approximately 25 grams a day. What % of this does every patient get on average?
--Query:
SELECT patientid,
     AVG(dietary_fiber) AS avg_fiber,
    (AVG(dietary_fiber)/25)*100 AS percentage_fiber
    FROM foodlog
    GROUP BY patientid
    ORDER BY patientid; 

--Q31 What is the relationship between EDA and Mean HR?
--Query:
SELECT corr(e.mean_eda, t2.mean_hr)
FROM eda e
     JOIN hr t2
         ON e.patientid = t2.patientid;
 

--Q32 Show the patient that spent the maximum time out of range.
--Query:
SELECT patientid,EXTRACT(EPOCH FROM (MAX(datestamp) - MIN(datestamp)))/3600 AS time_outside_range
FROM dexcom
WHERE
    (glucose_value_mgdl < 70 OR glucose_value_mgdl >= 126)
    GROUP BY patientid
    ORDER BY patientid
    LIMIT 1; 


--Q33 : Create a User Defined function that returns min glucose value and patient ID for any date entered
-- Query
 
DROP FUNCTION if exists minGlucose;
DROP TABLE if exists min_glucose_dexcom;
 
CREATE TABLE min_glucose_dexcom
( glucose_value_mgdl real,
  patientid integer);
 
CREATE FUNCTION minGlucose(dts date) RETURNS min_glucose_dexcom AS $$
  DELETE FROM min_glucose_dexcom;
 
  INSERT INTO min_glucose_dexcom
  SELECT MIN(glucose_value_mgdl),
    	patientid FROM dexcom
   WHERE date(datestamp) = dts and glucose_value_mgdl is not null
   GROUP BY patientid;
  
   SELECT glucose_value_mgdl,
      	patientid
   FROM min_glucose_dexcom;
 
$$ LANGUAGE SQL
;
-- Call the above procedure to display the patientid and minimum glucose value for a date.
SELECT * FROM minGlucose ('2020-02-16');
 
 
 
--Q34 : Write a query to find the day of highest mean HR value for each patient and display it along with the patient id.
--Query
 SELECT datestamp,
   	hr.patientid,
        	   mean_hr
FROM hr,
 	(SELECT MAX(mean_hr) AS mean_hr2 ,
        	  patientid from hr
        	  GROUP BY patientid) hr2
WHERE hr.patientid = hr2.patientid
AND hr.mean_hr = hr2.mean_hr2
        	 ;
 
 
--Q35: Create view to store Patient ID, Date, Avg Glucose value and Patient Day to every patient, ranging from 1-11 based on every patients minimum date and maximum date (eg: Day1,Day2 for each patient)
--Query
CREATE OR REPLACE VIEW patient_glucose_daily_summary AS
SELECT
	patientid,
	'Day ' || ROW_NUMBER() OVER (PARTITION BY patientid ORDER BY reading_date) AS patient_day,
        	date(reading_date) as reading_date,
	avg_glucose
   
FROM (
select
        	patientid,
        	date_trunc('day',datestamp) as reading_date,
        	avg(glucose_value_mgdl) as avg_glucose
from dexcom
group by
        	patientid,
        	reading_date
order by patientid, reading_date asc) subquery;
           
SELECT * FROM patient_glucose_daily_summary;
 
 
--Q36 : Using width bucket functions, group patients into 4 HRV categories
--Query
 
SELECT
	distinct
	CASE
    	WHEN bucket = 1 THEN '1. Low HRV'
    	WHEN bucket = 2 THEN '2. Moderate HRV'
    	WHEN bucket = 3 THEN '3. Moderately High HRV'
    	WHEN bucket = 4 THEN '4. High HRV'
    	ELSE 'Invalid HRV'
	END AS hrv_bucket, 	patientid,	hrv
FROM (
select patientid,  	hrv,  width_bucket(hrv, (select min(hrv) from hrv),(select max(hrv) from hrv),3) as bucket
from hrv
) AS hrv_bucketed
order by 1,3;
 
 
--Q37 : Is there a correlation between High EDA and HRV. If so, display this data by querying the relevant tables?
--Query
 
WITH HighEda AS (
	SELECT
    	e.patientid,
    	e.datestamp AS eda_timestamp,
    	e.max_eda as high_eda,
    	h.datestamp AS hrv_timestamp,
    	h.hrv
	FROM eda e
	JOIN hrv h ON e.patientid = h.patientid
	WHERE e.max_eda > 40
)
SELECT
	CORR(HighEda.high_eda, HighEda.hrv) AS correlation
FROM HighEda;
 
--Q38 : List hypoglycemic patients by age and gender
--Query
 
SELECT
	d.patientid,
	d.gender,
	date_part('year', age(current_date, d.dob)) AS age,
	dx.glucose_value_mgdl
FROM demographics d
JOIN dexcom dx ON d.patientid = dx.patientid
WHERE dx.glucose_value_mgdl < 70
ORDER BY age, gender;
 

--Q39 : Write a query using recursive view(use the given dataset only)
--Query
 
CREATE OR REPLACE RECURSIVE VIEW max_mean_hr (datestamp, patientid, mean_hr) AS
WITH RECURSIVE recursive_cte AS (
	SELECT
    	DATE(datestamp) AS datestamp,
    	patientid,
    	mean_hr
	FROM hr
	WHERE mean_hr > 100
	UNION ALL
	SELECT
    	DATE(hr2.datestamp) AS datestamp,
    	hr2.patientid,
    	hr2.mean_hr
	FROM hr hr2
	INNER JOIN recursive_cte r ON hr2.patientid = r.patientid
	WHERE hr2.mean_hr > 100
)
SELECT * FROM recursive_cte;
 
select * from max_mean_hr limit 10;
 

--Q40 : Create a stored procedure that adds a column to table IBI. The column should just be the date part extracted from IBI.Date
--Query

-- DROP Column if exists
ALTER TABLE IBI
drop column if exists date_part;
-- Create procedure
CREATE OR REPLACE PROCEDURE add_ibi_date()
LANGUAGE plpgsql
AS $$
BEGIN
   
  	  	-- Add the new column 'date_part' to the 'IBI' table
    	ALTER TABLE IBI ADD COLUMN date_part date;
 
    	-- Update the 'date_part' column with the date part from 'Date'
    	UPDATE IBI SET date_part = DATE(datestamp);
END;
$$;
 
CALL add_ibi_date();
 
select * from ibi;
 
 
--Q 41 : Fetch the list of Patient ID's whose sugar consumption exceeded 30 grams on a meal from FoodLog table.
--Query
SELECT DISTINCT patientid, MAX(sugar) as max_sugar
        	FROM foodlog
                    	WHERE sugar > 30
        	GROUP BY foodlog.patientid
        	ORDER BY patientid ASC;
 
--Q42 : How many patients are celebrating their birthday this month?
--Query
SELECT COUNT(patientid)
FROM demographics
WHERE
EXTRACT(MONTH FROM demographics.DOB)= EXTRACT(MONTH FROM current_date);
 
 
--Q43 : How many different types of events were recorded in the Dexcom tables? Display counts against each Event type
--Query
 
SELECT et.event_type, COUNT(*) AS event_count
FROM dexcom AS d
JOIN eventtype AS et ON d.eventid = et.id
GROUP BY et.event_type;
 
--Q44 : How many prediabetic/diabetic patients also had a high level of stress?
--Query
 
select count(diabetics.patientid)
from
(select patientid,
    	hba1c,
    	case when hba1c >= 6.5 then 'diabetic'
                    	 	when hba1c > 5.7 and hba1c < 6.5 then 'prediabetic'
                                	 else 'normal' end
                    	as diabetic_level
from demographics) diabetics,
(select patientid
--  	,max_eda
from eda
where max_eda > 40
union
select patientid
--  	,hrv
from hrv
where hrv < 20
union
select patientid
--   	,mean_hr
from hr
where mean_hr > 100
order by patientid) high_stress
where high_stress.patientid = diabetics.patientid
and diabetics.diabetic_level in ('prediabetic','diabetic');
 
--Q45 : List the food that coincided with the time of highest blood sugar for every patient.
--Query
 
WITH HighSugarFood AS (
	SELECT
    	patientid,
    	logged_food,
    	ROW_NUMBER() OVER (PARTITION BY PatientID ORDER BY sugar DESC) AS RowNum
	FROM
    	foodlog
)
SELECT
	patientid,
        	logged_food   
FROM
	HighSugarFood
WHERE
	RowNum = 1;
 
 
--Q46 : How many patients have first names with length >7 letters?
 --Query
 
SELECT COUNT(*) AS long_names
FROM demographics
WHERE LENGTH(firstname) > 7;
 
--Q47 : List all foods logged that end with 'se'. Ensure that the output is in Title Case.
 --Query
 
SELECT INITCAP(logged_food)
FROM foodlog
WHERE logged_food LIKE '%se';
 
--Q48  List the patients who had a birthday the same week as their glucose or IBI readings.
--Query
 
SELECT DISTINCT D.patientid, D.DOB
FROM demographics D 
INNER JOIN
dexcom Dx ON D.PatientID = Dx.PatientID AND EXTRACT(WEEK FROM D.DOB) = EXTRACT(WEEK FROM Dx.datestamp)
UNION
SELECT DISTINCT D.patientid, D.DOB
FROM demographics D
INNER JOIN
ibi I ON D.PatientID = I.PatientID AND EXTRACT(WEEK FROM D.DOB) = EXTRACT(WEEK FROM I.datestamp);
 
--Q49: Assuming breakfast is between 8 am and 11 am. How many patients ate a meal with bananas in it?
--Query:

SELECT count( distinct patientid) as total_patients from  foodlog
where  Extract(hour from datetime) 
between 8 and 11 and logged_food = 'Banana' 

--Q50: Create a User defined function that returns the age of any patient based on input
--Query

create  or replace function get_age(inputid bigint)
returns int
language plpgsql
as
$$
declare 
age int;
begin
select  DATE_PART('YEAR', age(current_date, dob))
into age
from demographics
where patientid=inputid;
return age;
end;
$$

select  get_age(1)


--Q51:Based on Number of hyper and hypoglycemic incidents per patient, which patient has the least control over their blood sugar
--Query

select 
	concat(firstname,' ',lastname),
	demographics.patientid,
	count( demographics.patientid )
from dexcom, demographics
where dexcom.glucose_value_mgdl<70 or dexcom.glucose_value_mgdl>=126 
group by demographics.patientid 
order by count(demographics.patientid) desc limit 1  


--Q52: Display patients details with event details and minimum heart rate
--Query

select concat(firstname,' ',lastname),
	demographics.patientid,
	eventtype.event_type,
	eventtype.event_subtype,
	eventtype.glucose_value_mgdl,
	eventtype.id,hr.min_hr 
from demographics
inner join dexcom on demographics.patientid= dexcom.patientid 
inner join eventtype on dexcom.eventid= eventtype.id 
inner join hr on dexcom.patientid=hr.patientid 
order by hr.min_hr asc limit 1

--Q53: Display a list of patients whose daily max_eda lies between 40 and 50
--Query

select concat(firstname,' ',lastname),demographics.patientid, eda.max_eda 
from demographics 
inner join eda on demographics.patientid = eda.patientid
where eda.max_eda
between 40 and 50

--Q54:Count the number of hyper and hypoglycemic incidents per patient
--Query

select patientid, count(glucose_value_mgdl)
from dexcom
where glucose_value_mgdl<70 or glucose_value_mgdl>=126 
group by patientid

--Q55: What is the variance from mean  for all patients for the table IBI?
--Query

select variance(mean_ibi_ms) from ibi

--Q56:Create a view that combines all relevant patient demographics and lab markers into one. Call this view ‘Patient_Overview’
--Query

create or replace view Patient_Overview as
select concat(firstname,' ',lastname),
	demographics.patientid,
	demographics.gender , 
	dexcom.glucose_value_mgdl, 
	eda.mean_eda,
	hr.mean_hr
from demographics, dexcom, eda, hr
where 
 dexcom.patientid = demographics.patientid
 and eda.patientid =demographics.patientid
 and hr.patientid = demographics.patientid

select * from Patient_Overview

--Q57:Create a table that stores an array of biomarkers: Min(Glucose Value), Avg(Mean_HR), Max(Max_EDA) for every patient. The result should look like this: (Link in next cell) 
--Query

create table newtable(
	pid int,
 biomarkers numeric [] 
)

insert into newtable
select dexcom.patientid,array[ min(dexcom.glucose_value_mgdl),avg(hr.mean_hr),max(eda.max_eda)] 
from dexcom inner join hr on dexcom.patientid = hr.patientid inner join eda on hr.patientid=eda.patientid
group by dexcom.patientid

select * from newtable order by pid

drop table newtable

--Q58:Assuming lunch is between 12pm and 2pm. Calculate the total number of calories consumed by each patient for lunch on "2020-02-24" 
--Query
select patientid, sum(calorie) as total_calorie from foodlog
where
((Extract(hour from datetime) between 12 and 14) and  cast(datetime as date) = '2020-02-24')
group by patientid


--Q 59:What is the total length of time recorded for each patient(in hours) in the Dexcom table?
--Query 

SELECT
    patientid,
    EXTRACT(EPOCH FROM (MAX(datestamp) - MIN(datestamp))) / 3600 AS time_in_hours
FROM dexcom
GROUP BY patientid
ORDER BY patientid ASC;

--Q 60:Display the first, last name, patient age and max glucose reading in one string for every patient
--Query 

select demographics.patientid, 
demographics.firstname||' '||demographics.lastname||' '||
DATE_PART('YEAR', age(current_date, dob))||' '||max(glucose_value_mgdl) as details
from demographics, dexcom  
group by demographics.patientid

--Q 61:What is the average age of all patients in the database?
--Query 
select  avg(DATE_PART('YEAR', age(current_date, dob)))
from demographics 

--Q  62:Display All female patients with age less than 50?
--Query 

select * from demographics 
where gender= 'FEMALE' and DATE_PART('YEAR', age(current_date, dob))< 50

--Q 63:Display count of Event ID, Event Subtype and the first letter of the event subtype. Display all events
--Query 

select eventtype.id,count(eventtype.id), 
eventtype.event_subtype,
left(eventtype.event_subtype,1) as firstletter 
from eventtype 
inner join dexcom on eventtype.id=dexcom.eventid 
group by eventtype.id

--Q 64:List the foods consumed by  the patient(s) whose eventype is "Estimated Glucose Value
--Query 
with cte as(
 SELECT
            dexcom.patientid, eventtype.event_subtype as eventtype
        FROM
            dexcom
        INNER JOIN eventtype ON dexcom.eventid = eventtype.id
        WHERE
            eventtype.event_subtype = 'Estimated Glucose Value')
SELECT
    distinct foodlog.patientid,
    foodlog.logged_food,
	cte.eventtype
FROM
    foodlog, cte
WHERE
    foodlog.patientid = cte.patientid
	order by patientid asc

--Q65:	Rank the patients' health based on HRV score and Control of blood sugar(AKA min time spent out of range)	
--Query:	
WITH HRV_Scores AS (
    SELECT
        patientid,
        ROUND(hrv::numeric, 2)  AS hrv_score
    FROM ibi
    GROUP BY patientid,hrv
),
Blood_Sugar_Control AS (
    SELECT
        d.patientid,
        SUM(CASE
            WHEN glucose_value_mgdl <55 OR glucose_value_mgdl >200 THEN 1
            ELSE 0
        END) AS time_out_of_range
    FROM demographics d
    JOIN dexcom g ON d.patientid = g.patientid
    GROUP BY d.patientid
)
SELECT
    d.patientid,
    CONCAT(d.firstname, ' ', d.lastname) AS patients_full_name,
    COALESCE(h.hrv_score, 0) AS hrv_score,
    COALESCE(b.time_out_of_range, 0) AS time_out_of_range,
    (COALESCE(h.hrv_score, 0) + COALESCE(b.time_out_of_range, 0)) AS combined_score,	
    CASE
        WHEN (COALESCE(h.hrv_score, 0) + COALESCE(b.time_out_of_range, 0)) > 50 THEN 'poor health'
        ELSE 'better health'
    END AS health_status,
	 ROW_NUMBER() OVER (ORDER BY  (COALESCE(h.hrv_score, 0) + COALESCE(b.time_out_of_range, 0)) ASC) AS health_rank
FROM demographics d
LEFT JOIN HRV_Scores h ON d.patientid = h.patientid
LEFT JOIN Blood_Sugar_Control b ON d.patientid = b.patientid
ORDER BY combined_score,patientid ;

--Q66. Create a trigger on the food log table that warns a person about any food logged that has more than 20 grams of sugar. The user should not be stopped from inserting the row. Only a warning is needed	
--Query:
--Trigger function:
CREATE OR REPLACE FUNCTION check_sugar_content()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.sugar > 20 THEN
        RAISE NOTICE 'Warning: High sugar content([%]) detected in the logged food ', NEW.sugar;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--Trigger :
CREATE OR REPLACE TRIGGER foodlog_sugar_check
BEFORE INSERT ON foodlog
FOR EACH ROW
EXECUTE FUNCTION check_sugar_content();

-- trying to insert a record with less sugar content should trigger the trigger function
insert into foodlog(datetime,logged_food,calorie, total_carb, dietary_fiber, sugar,protein,total_fat,patientid) values(CURRENT_TIMESTAMP,'Berry juice',456,90,2,45,23,3,1)


--Q67.Display all the patients with high heart rate and prediabetic	
--Query:							
SELECT
    d.patientid, 
    d.gender,
    d.hba1c ,
	case
	  WHEN d.hba1c <5.7 then 'normal' 
	  WHEN d.hba1c >5.7 and d.hba1c<6.4 then 'prediabetic' 
	  WHEN  d.hba1c>6.4 then 'diabetic' 
	  ELSE 'invalid range'
	  END RANGE,
    h.mean_hr AS heart_rate
FROM
    demographics d
JOIN
    hr h ON d.patientid = h.patientid
WHERE
    h.mean_hr > 100 
    AND 
	d.hba1c BETWEEN 5.7 AND 6.4; -- prediabetic range is between 5.7% and 6.4% as mentioned ----in data definition


--Q68.	Display patients information who have tachycardia HR and a glucose value greater than 200.
--Query:				
SELECT 
	d.patientid,
	d.gender,
	h.mean_hr as tachycardia_HR,
	glucose_value_mgdl as glucose_level
FROM 
	demographics d
JOIN
	hr h ON d.patientid=h.patientid
JOIN
    dexcom dx ON dx.patientid=d.patientid
WHERE
	h.mean_hr>100 and dx.glucose_value_mgdl >200;


--Q69.	Calculate the number of hypoglycemic incident per patient per day where glucose drops under 55									Query:															
--Query
SELECT
    d.patientid,
    DATE(dx.datestamp) AS day,
    SUM(CASE WHEN dx.glucose_value_mgdl < 55 THEN 1 ELSE 0 END) AS hypoglycemic_incidents
FROM
    demographics d
JOIN
    dexcom as dx ON d.patientid = dx.patientid
GROUP BY
    d.patientid, DATE(dx.datestamp)
HAVING
    SUM(CASE WHEN dx.glucose_value_mgdl < 55 THEN 1 ELSE 0 END) > 0
ORDER BY
    d.patientid, day;



--Q70. List the day wise calories intake for each patient.	
--Query:
WITH UniqueDates AS (
    SELECT DISTINCT patientid, datetime::DATE AS unique_date,	
	SUM(calorie)::NUMERIC AS cal
    FROM foodlog
	GROUP BY patientid, unique_date
	ORDER BY patientid
)
SELECT
    f.patientid,  
    ud.unique_date,
	'Day' || ROW_NUMBER() OVER (PARTITION BY ud.patientid ORDER BY ud.unique_date) AS patient_day,
    ud.cal AS total_calories
FROM
     uniquedates ud
JOIN
    foodlog f ON f.patientid = ud.patientid
GROUP BY
    f.patientid,ud.unique_date,ud.patientid,ud.cal
ORDER BY
    f.patientid; 


--Q71. Display the demographic details for the patient that had the maximum time below recommended blood glucose range		
--Query:
WITH time_below_range AS (
    SELECT
        patientid,
        SUM(CASE WHEN glucose_value_mgdl<60 THEN 1 ELSE 0 END) AS count_below_range	-- count(patientid) AS time_below_range
    FROM
        dexcom
    GROUP BY
        patientid
)
SELECT
    d.patientid,
    d.gender,
    d.hba1c,
    d.dob  
FROM
    time_below_range tbr
JOIN
    demographics d ON tbr.patientid = d.patientid
WHERE
    tbr.count_below_range = (SELECT MAX(count_below_range) FROM time_below_range);



--Q72. How many patients have a minimum HR below the medically recommended level?	
--Query:													
SELECT  count(*) as minimum_hr_patient_count 
FROM 
	hr h
JOIN
	demographics d ON d.patientid=h.patientid
WHERE
	h.min_hr <60
ORDER BY 
	minimum_hr_patient_count  asc

	
-- Q73.	Create a trigger to raise notice and prevent the deletion of a record from ‘Patient_Overview’ .	
--Create View:
CREATE or replace VIEW Patient_Overview AS
SELECT
    d.patientid,
    concat (d.firstname,' ',d.lastname),
    d.gender,
    d.hba1c,
    d.dob,
    ROUND(CAST(h.mean_hr AS NUMERIC), 2) AS average_heart_rate,
    coalesce(g.glucose_value_mgdl,0) AS last_glucose_reading
FROM demographics d
LEFT JOIN hr h ON d.patientid = h.patientid
LEFT JOIN dexcom g ON d.patientid = g.patientid


--Trigger Function:
-- Create an INSTEAD OF DELETE trigger on the 'demographics' table
CREATE OR REPLACE FUNCTION prevent_patient_overview_deletion() RETURNS TRIGGER AS $$
BEGIN  
    RAISE NOTICE 'Deletion from the "Patient_Overview" view is not allowed. Delete records from the underlying tables.';    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


--Trigger
-- Create the INSTEAD OF DELETE trigger
CREATE or REPLACE TRIGGER prevent_patient_overview_delete_trigger
INSTEAD OF DELETE ON Patient_Overview
FOR EACH ROW
EXECUTE FUNCTION prevent_patient_overview_deletion();


--Delete Event:
delete from patient_overview where patientid=2
OUTPUT:


 Q74. What is the average heart rate, age and gender of the every patient in the dataset?
--Query:
SELECT 
     d.patientid,
	 d.gender,
	 date_part('year',age(d.dob)) as patient_age,
--	 EXTRACT(year FROM age(current_date,d.dob)) :: int as age-- other way to extract age
 	 round(cast(AVG(h.mean_hr) as NUMERIC), 2) AS patient_average_heart_rate
FROM
    demographics d
JOIN
 	hr h ON d.patientid = h.patientid
GROUP BY
    d.patientid, d.gender,patient_age
ORDER BY
    d.patientid; 


--Q75. What is the daily total calories consumed by every patient?
--Query
WITH UniqueDates AS (
    SELECT DISTINCT patientid, datetime::DATE AS unique_date,	
	sum(calorie)::NUMERIC AS cal
    FROM foodlog
	group by patientid, unique_date
	order by patientid
)
SELECT
    f.patientid,  
    ud.unique_date,
	'Day' || ROW_NUMBER() OVER (PARTITION BY ud.patientid ORDER BY ud.unique_date) AS patient_day,
    ud.cal ||' '||'calories' AS total_calories
FROM
     uniquedates ud
JOIN
    foodlog f ON f.patientid = ud.patientid
GROUP BY
    f.patientid,ud.unique_date,ud.patientid,ud.cal
ORDER BY
    f.patientid;



 --Q76. Write a query to classify max EDA into 5 categories and display the number of patients in each category.
--Query:
-- Category Table for EDA
WITH EDA_Category AS (
  SELECT eda.patientid, eda.max_eda,
    CASE
      WHEN eda.max_eda <= 10 THEN 'Low'
      WHEN eda.max_eda > 10 AND eda.max_eda <= 20 THEN 'Moderate'
      WHEN eda.max_eda > 20 AND eda.max_eda <= 30 THEN 'High'
      WHEN eda.max_eda > 30 AND eda.max_eda <= 40 THEN 'Very High'
      ELSE 'Extreme'
    END AS EDAcategory
  FROM eda
)

-- Number of patients in each category display
SELECT EDAcategory, COUNT(patientid) AS number_of_patients
FROM EDA_Category
GROUP BY EDAcategory
ORDER BY EDAcategory;
output:


--Q77.List the daily max HR for patient with event type Exercise.
--Query:
SELECT
    h.patientid,
    e.event_type,
    h.datestamp AS date,
    MAX(h.max_hr) AS daily_max_hr
FROM
    hr h
JOIN
   demographics d ON h.patientid = d.patientid
JOIN
   dexcom dx ON dx.patientid=d.patientid
JOIN 
   eventtype e ON e.id=dx.eventid
WHERE
    e.event_type = 'Exercise'
GROUP BY
    h.patientid, e.event_type, h.datestamp
ORDER BY
    h.patientid, h.datestamp;



--Q78.What is the standard deviation from mean for all patients for the table HR?
--Query:
SELECT STDDEV(mean_hr) AS standard_deviation
FROM hr h


--Q79.Give the demographic details of the patient with event type ID of 16.
--Query:
SELECT d.patientid, d.gender, d.hba1c, d.dob, d.firstname, d.lastname
FROM demographics d
JOIN dexcom dx ON d.patientid = dx.patientid
JOIN eventtype e ON dx.eventid = e.id
WHERE e.id = 16;



--Q80.Display list of patients along with their gender having a tachycardia mean HR.
--Query
SELECT d.patientid,d.firstname, d.lastname,d.gender, h.mean_hr as heartrate_greaterthen100
FROM demographics d
JOIN hr h ON d.patientid=h.patientid
WHERE h.mean_hr>100 and h.mean_hr<250





