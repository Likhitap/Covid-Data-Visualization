-- Checking the running total of cases by location 

SELECT Location, date,
SUM(CONVERT(bigint, new_cases)) OVER (Partition by Location Order By Location, date) AS RunningtotalCases
FROM PortfolioProjects..CovidDeaths

--Checking all the locations that have more than 200,000,000 cases (nested subquery)

Select Location, MAX(total_cases) as total_cases
FROM PortfolioProjects..CovidDeaths
WHERE Location IN (SELECT Location 
                   FROM PortfolioProjects..CovidVaccination
				   Where  CONVERT(bigint,total_vaccinations) > 200000000 AND continent is not null)
GROUP BY Location
ORDER BY total_cases DESC

-- Common Table Expression (CTE)

-- Total Population and Vaccination CTE

WITH PopvsVac (continent, Location, date, Population, New_Vaccinations, RollingPeopleVaccinated)
as
(
SELECT D.continent, D.Location, D.date, D.Population, V.new_vaccinations,
SUM(CONVERT(bigint,V.new_vaccinations)) OVER (Partition by D.Location Order By D.Location, D.date) AS RollingPeopleVaccinated
FROM PortfolioProjects..CovidDeaths AS D
JOIN PortfolioProjects..CovidVaccination AS V
	ON D.location = V.location
	AND D.date = V.date
WHERE D.continent is not null
)

SELECT Location,date, Population, RollingPeopleVaccinated
FROM PopvsVac
WHERE location = 'United States'

-- Checking the location with Maximum total cases

SELECT Location, MAX(CONVERT(int,total_cases)) As Total_Cases, MAX(CONVERT(int,total_deaths)) As Total_Deaths
FROM PortfolioProjects..CovidDeaths
WHERE continent is not null
GROUP BY Location
ORDER BY Total_Cases DESC


--Exploring the covid deaths and covid vaccination dataset
SELECT * 
FROM PortfolioProjects..CovidDeaths

-- Check the cases in United States
SELECT * 
FROM PortfolioProjects..CovidDeaths
WHERE location = 'United States'

-- Checking the death percentage in United States

SELECT Location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as DeathPercentage
FROM PortfolioProjects..CovidDeaths
WHERE location = 'United States'
ORDER BY Date

-- Checking the vaccination data of United Sates
SELECT *
FROM PortfolioProjects..CovidVaccination
WHERE location = 'United States'

-- Checking the Percentage of population that is vaccinated in United Sates
SELECT D.Location, D.date, (V.total_vaccinations / D.population)*100 as VaccinationPercentage
FROM PortfolioProjects..CovidDeaths AS D
JOIN PortfolioProjects..CovidVaccination AS V
    ON D.Location = V.Location
	AND D.date = V.date
WHERE D.Location = 'United States'
ORDER BY date

-- Joining the covid vaccination and covid deaths table to check the new death vs new vaccination

SELECT D.Location, D.date, D.new_deaths, V.new_vaccinations
FROM PortfolioProjects..CovidDeaths AS D
JOIN PortfolioProjects..CovidVaccination AS V
	ON D.location = V.location
	AND D.date = V.date
WHERE D.location = 'United States'

--Creating view for further analysis

CREATE VIEW CombinedCovidData AS
SELECT D.continent, D.location, D.date, D.population,ISNULL(D.total_cases,0) as total_cases, ISNULL(D.new_cases,0) as new_cases,ISNULL(D.total_deaths,0) as total_deaths, ISNULL(D.new_deaths,0) as new_deaths, D.reproduction_rate, D.icu_patients, D.hosp_patients,
ISNULL(V.total_tests,0) as total_tests, ISNULL(V.new_tests,0) as new_tests, ISNULL(V.total_vaccinations,0) as total_vaccinations, ISNULL(V.new_vaccinations,0) as new_vaccinations, V.positive_rate, ISNULL(v.people_vaccinated,0) as people_vaccinated,
ISNULL(V.people_fully_vaccinated,0) as people_fully_vaccinated,ISNULL(V.total_boosters,0) as total_boosters, V.gdp_per_capita, V.cardiovasc_death_rate, V.life_expectancy
FROM PortfolioProjects..CovidDeaths AS D
JOIN PortfolioProjects..CovidVaccination AS V
	ON D.location = V.location
	AND D.date = V.date
WHERE D.continent is not null