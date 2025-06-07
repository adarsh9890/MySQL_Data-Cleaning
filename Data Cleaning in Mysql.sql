-- Data Cleaning


SELECT*
FROM layoffs;

-- 1.Remove Duplicates
-- 2.Standardize the data
-- 3.Null Values or blank values
-- 4.Remove any columns


CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT*
FROM layoffs;

SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
select*
from duplicate_cte
where row_num>1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT*
FROM layoffs_staging2
where row_num>1;

DELETE 
from layoffs_staging2
where row_num>1;

-- 2.Standardize the data

select*
from layoffs_staging2;

select company,trim(company)
from layoffs_staging2;

update layoffs_staging2
set company=trim(company);

select distinct industry
from layoffs_staging2;

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry='Crypto'
where industry like 'Crypto%';

select distinct country ,trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country=trim(trailing '.' from country)
where country like 'United States%';


select distinct country
from layoffs_staging2
order by 1;

select `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date`=STR_TO_DATE(`date`,'%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging2
set industry=null
where industry='';

select *
from layoffs_staging2
where industry is null
or industry='';

select *
from layoffs_staging2
where company='Airbnb';

update layoffs_staging2 t1
join layoffs_staging2 t2
   on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null
and t2.industry is not null;

select  t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging t2
   on t1.company=t2.company
where (t1.industry is null or t1.industry='')
and t2.industry is not null;


delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

select *
from layoffs_staging2;
