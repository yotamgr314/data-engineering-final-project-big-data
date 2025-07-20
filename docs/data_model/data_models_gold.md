## Gold Layer – Aggregated and Analytical Data Tables

The gold layer contains aggregated metrics and analytical tables for reporting, business insights, and machine learning model training.

---

### `GOLD_DIM_MONTHLY_DELAYS`  
Delay metrics per month, including statistics and most common reasons.

### `GOLD_DIM_MONTHLY_FLIGHT_INFO`  
Monthly flight activity metrics including total flights and passenger numbers.

### `GOLD_DIM_MONTHLY_REVENUE`  
Monthly revenue metrics including ticket class revenue breakdown and cancelation rate.

### `GOLD_DIM_MONTHLY_MOST_POPULAR`  
Most frequent metrics per month including popular destinations and booking methods.

### `GOLD_FACT_MONTHLY_PERFORMANCE_REPORT`  
Central fact table linking monthly delay, revenue, traffic, and popularity dimensions.

### `GOLD_MONTHLY_AGGREGATES`  
Flat aggregated monthly metrics used for simplified reporting and dashboarding.

### `GOLD_DIM_ML_ROUTES`  
Route dimension used for ML training, including flight time and distance.

### `GOLD_DIM_ML_WEATHER`  
Aggregated weather metrics per route for ML training context.

### `GOLD_DIM_ML_BOARDING_DATA`  
Flight capacity and luggage data used in machine learning.

### `GOLD_FACT_ML_TRAINING`  
Fact table for ML model training containing engineered features and target delay.