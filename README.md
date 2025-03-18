# Spark Structured API: Movie Ratings Analysis

## Overview
This project leverages **Apache Spark Structured APIs** to analyze a dataset containing **movie ratings, user engagement, and streaming behavior**. The goal is to extract insights related to:
- **Binge-watching patterns**
- **Churn risk users**
- **Long-term streaming trends**

## Dataset
The dataset (`movie_ratings_data.csv`) contains the following columns:

| Column Name         | Data Type | Description |
|---------------------|-----------|-------------|
| UserID             | Integer   | Unique identifier for a user |
| MovieID           | Integer   | Unique identifier for a movie |
| MovieTitle        | String    | Name of the movie |
| Genre             | String    | Movie genre (e.g., Action, Comedy, Drama) |
| Rating           | Float     | User rating (1.0 to 5.0) |
| ReviewCount      | Integer   | Total reviews given by the user |
| WatchedYear      | Integer   | Year when the movie was watched |
| UserLocation     | String    | User's country |
| AgeGroup        | String    | Age category (Teen, Adult, Senior) |
| StreamingPlatform | String    | Platform where the movie was watched |
| WatchTime       | Integer   | Total watch time in minutes |
| IsBingeWatched  | Boolean   | `True` if the user watched 3+ movies in a day |
| SubscriptionStatus | String    | Subscription status (Active, Canceled) |

## Assignment Tasks

### **1. Detect Binge-Watching Patterns**
**Objective:** Identify which age groups binge-watch movies the most.

#### **Steps Implemented:**
1. **Filter Users:** Selected users where `IsBingeWatched = True`.
2. **Group by Age Group:** Counted the number of binge-watchers in each age category.
3. **Calculate Proportions:** Computed the percentage of binge-watchers per age group.

#### **Output:**
```
AgeGroup,Binge Watchers,Total Users,Percentage
Senior,20,37,54.05
Adult,13,27,48.15
Teen,15,36,41.67
```
#### **Run the Task:**
```bash
spark-submit task1_binge_watching_patterns.py
```

---
### **2. Identify Churn Risk Users**
**Objective:** Identify users at risk of churn by detecting those with canceled subscriptions and low watch time (<100 minutes).

#### **Steps Implemented:**
1. **Filter Users:** Selected users with `SubscriptionStatus = 'Canceled'` and `WatchTime < 100`.
2. **Count At-Risk Users:** Computed the total number of such users.
3. **Formatted Output:** Matched the required format with a description column.

#### **Expected Output:**
```
Churn Risk Users,Total Users
Users with low watch time & canceled subscriptions,14

```
#### **Run the Task:**
```bash
spark-submit task2_churn_risk_users.py
```

---
### **3. Trend Analysis Over the Years**
**Objective:** Analyze how movie-watching trends have changed over the years and find peak years for streaming activity.

#### **Steps Implemented:**
1. **Group by Watched Year:** Counted the number of movies watched per year.
2. **Analyze Trends:** Ordered the results by `WatchedYear`.

#### **Expected Output:**
```
WatchedYear,Movies Watched
2018,17
2019,12
2020,13
2021,17
2022,18
2023,23

```
#### **Run the Task:**
```bash
spark-submit task3_movie_watching_trends.py
```

---
## **Project Execution**
### **Setup & Initial Commands Run:**
```bash
python generate_dataset.py  # Generate dataset
pip install pyspark  # Install dependencies
docker-compose up -d  # Start Docker environment (if applicable)
```
### **Run Spark Jobs:**
```bash
spark-submit task1_binge_watching_patterns.py
spark-submit task2_churn_risk_users.py
spark-submit task3_movie_watching_trends.py
```
### **Commit & Push Code to GitHub:**
```bash
git add .
git commit -m "Assignment"
git push origin main
```

## **Outputs Directory:**
After running all scripts, the results are stored in the `outputs/` directory:
```bash
ls outputs/
# Output files
binge_watching_patterns.csv
churn_risk_users.csv
movie_watching_trends.csv
```
To check results:
```bash
cat outputs/binge_watching_patterns.csv
cat outputs/churn_risk_users.csv
cat outputs/movie_watching_trends.csv
```

## **Final Notes**
✔️ Ensure **Spark is running** before executing scripts.
✔️ **Dataset must contain at least 100 records** for meaningful analysis.
✔️ **Results match the expected format and are stored in `outputs/`.**

This completes the **Spark Structured API: Movie Ratings Analysis** project. 🚀

