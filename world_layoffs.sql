select * from layoffs;

create table layoffs_staging
like layoffs;

select * from layoffs_staging;

Insert layoffs_staging
select * from layoffs;

-- REMOVE DUPLICATES

select *,
Row_Number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
from layoffs_staging;

with duplicate_cte as 
(
select *,
Row_Number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
from layoffs_staging
)  
select *
from duplicate_cte;
-- where row_num > 1;

-- select * 
-- from layoffs_staging
-- where company = 'Casper'; 


create table layoffs_staging2 (
company text,
location text,
industry text,
total_laid_off int default null,
percentage_laid_off text,
`date` text,
stage text,
country text,
funds_raised_millions int default null,
row_num int
) ENGINE = InnoDB Default CHARSET=utf8mb4 Collate=utf8mb4_0900_ai_ci;


select * from layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, 'date' , stage
, country, funds_raised_millions) AS row_num
FROM layoffs_staging;



DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- FINDING ISSUES IN THE DATA AND FIXING IT (STANDARDIZE DATA)

SELECT company, trim(company)
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry 
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET industry = 'crypto'
WHERE industry LIKE 'CRYPTO%';


SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
Order by 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United states%';


SELECT `date`
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y') 

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- NULL VALUES OR BLANK VALUES

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR  industry = ''; 

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
   ON t1.company = t2.company
 WHERE (t1.industry IS NULL OR t1.industry = '')
 AND t2.industry IS NOT NULL;
 
 UPDATE layoffs_staging2
 SET industry = NULL
 WHERE industry = ' ';

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- REMOVE UNNECCESARY ROWS AND COLUMNS 

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;








    
 
 
 
 
 



























