-- SQL PROJECT - DATA CLEANING 

--  https://www.kaggle.com/datasets/swaptr/layoffs-2022

Select* from world_layoff.layoffs;

-- first thing to do is creating a staging table

CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT world_layoffs.layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

Select* from world_layoff.layoffs_staging;

-- 1. Remove Duplicates

# checking for duplicates


SELECT company, industry, total_laid_off, date,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,date) AS row_num
	FROM 
		world_layoff.layoffs_staging;
        
SELECT *
FROM (
	SELECT company, industry, total_laid_off, date,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off, date
			) AS row_num
	FROM 
		world_layoff.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
    
-- looking at oda to confirm
SELECT *
FROM world_layoff.layoffs_staging
WHERE company = 'Oda'
;
-- these are the real duplicates on the table

SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off, date, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off, date, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoff.layoffs_staging
) duplicates
WHERE 
	row_num > 1
    ;
   -- these are the ones we want to delete where the row number is > 1 or 2or greater essentially 

WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions)
    AS row_num
	FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

--  creating a new column and adding those row numbers in. Then delete where row numbers are over 2, then delete the column

ALTER TABLE world_layoffs.layoffs_staging 
ADD row_num INT;

SELECT *
FROM world_layoffs.layoffs_staging
;

CREATE TABLE `world_layoff`.`layoffstaging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text, 
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mba_0900_ai_ci;

SELECT * FROM world_layoff.layoffs_staging2;

INSERT INTO world_layoff.layoffs_staging2
SELECT  
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off, date, stage, country, funds_raised_millions
			) AS row_num
            FROM 
            layoffs_staging;
            
--  Deleting rows were row_num is greater than 2

DELETE FROM world_layoff.layoffs_staging
WHERE row_num >= 1;

---------------------------------------------------------------------------------------------------------------------------------------------------

-- 2. Standardize Data

SELECT * FROM world_layoff.layoffs_staging2;

SELECT DISTINCT industry
FROM world_layoff.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoff.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- now if we check those are all null

SELECT *
from  world_layoffs.layoffs_staging2
WHERE industry is NULL 
OR industry = '';

UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';
 
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
 ON t1.company=t2.company
 WHERE (t1.industry IS NULL OR t1.industry = '')
 AND t2.industry IS NOT NULL;
 
 UPDATE layoffs_staging2 t1
 JOIN layoffs_staging2 t2
  ON t1.company=t2.company
  SET t1. industry = t2.industry
   WHERE t1.industry IS NULL 
 AND t2.industry IS NOT NULL;


-- standardizing Crypto
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;


SELECT *
FROM world_layoffs.layoffs_staging2;

-- Standardizing United State. to United State

SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed

SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

-- fixing the date columns

SELECT date
from world_layoff.layoffs_staging2;

SELECT date,
STR_TO_DATE(date, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
SET Date = STR_TO_DATE(date, '%m/%d/%Y');

ALTER Table layoffs_staging2
Modify COLUMN  date Date;
--------------------------------------------------------------------------------------------------------------------------------------------------

 -- 3. Looking at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values

--------------------------------------------------------------------------------------------------------------------------------------------------

-- 4. Removing any columns and rows 

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
from layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data that can't really use
DELETE
from layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
from world_layoff.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP column row_num;

SELECT *
from world_layoff.layoffs_staging2;



 






