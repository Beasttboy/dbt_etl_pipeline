# Snowflake + dbt + Airflow: ELT Pipeline

This repository contains a complete ELT (Extract, Load, Transform) pipeline. It leverages **Snowflake** as the data warehouse, **dbt (data build tool)** for modular SQL transformations, and **Apache Airflow (via Cosmos)** for orchestration.

## 🏗️ Architecture
1.  **Extract & Load**: Raw TPC-H data is accessed via Snowflake Sample Data.
2.  **Staging Layer**: Cleaned and renamed views of raw tables.
3.  **Intermediate Layer**: Complex joins and business logic (utilizing dbt macros).
4.  **Mart Layer**: Production-ready Fact tables for BI and analysis.
5.  **Orchestration**: Airflow automates the dbt run/test cycle using a Dockerized environment.

## 🚀 Getting Started

### Prerequisites
* A Snowflake Account.
* Docker & Docker Compose installed.
* dbt Core installed locally (for development).

### Step 1: Snowflake Setup
Execute the following SQL in your Snowflake worksheet to initialize the environment:
```sql
-- create accounts
use role accountadmin;

create warehouse dbt_wh with warehouse_size='x-small';
create database if not exists dbt_db;
create role if not exists dbt_role;

show grants on warehouse dbt_wh;

grant role dbt_role to user jayzern;
grant usage on warehouse dbt_wh to role dbt_role;
grant all on database dbt_db to role dbt_role;

use role dbt_role;

create schema if not exists dbt_db.dbt_schema;

-- clean up
use role accountadmin;

drop warehouse if exists dbt_wh;
drop database if exists dbt_db;
drop role if exists dbt_role;
```

### Step 2: dbt Project Structure
Models: Modular SQL files organized by staging, intermediate, and marts.

Macros: Reusable logic located in macros/pricing.sql (e.g., discounted_amount).

**Tests:**

Generic: Schema tests (unique, not_null) in .yml files.

Singular: Custom business logic tests in the tests/ folder.

#### Step 2.1: Configure dbt_profile.yaml

```yaml
models:
  snowflake_workshop:
    staging:
      materialized: view
      snowflake_warehouse: dbt_wh
    marts:
      materialized: table
      snowflake_warehouse: dbt_wh
```

#### Step 2.2: Create source and staging files

Create `models/staging/tpch_sources.yml`

```yaml
version: 2

sources:
  - name: tpch
    database: snowflake_sample_data
    schema: tpch_sf1
    tables:
      - name: orders
        columns:
          - name: o_orderkey
            tests:
              - unique
              - not_null
      - name: lineitem
        columns:
          - name: l_orderkey
            tests:
              - relationships:
                  to: source('tpch', 'orders')
                  field: o_orderkey

```

Create staging models `models/staging/stg_tpch_orders.sql`

```sql
select
    o_orderkey as order_key,
    o_custkey as customer_key,
    o_orderstatus as status_code,
    o_totalprice as total_price,
    o_orderdate as order_date
from
    {{ source('tpch', 'orders') }}

```

Create `models/staging/tpch/stg_tpch_line_items.sql`

```sql
select
    {{
        dbt_utils.generate_surrogate_key([
            'l_orderkey',
            'l_linenumber'
        ])
    }} as order_item_key,
	l_orderkey as order_key,
	l_partkey as part_key,
	l_linenumber as line_number,
	l_quantity as quantity,
	l_extendedprice as extended_price,
	l_discount as discount_percentage,
	l_tax as tax_rate
from
    {{ source('tpch', 'lineitem') }}
```

#### Step 2.3: Macros (Don’t repeat yourself or D.R.Y.)

Create `macros/pricing.sql`

```sql
{% macro discounted_amount(extended_price, discount_percentage, scale=2) %}
    (-1 * {{extended_price}} * {{discount_percentage}})::decimal(16, {{ scale }})
{% endmacro %}
```

#### Step 2.4: Transform models (fact tables, data marts)

Create Intermediate table `models/marts/int_order_items.sql` 

```sql
select
    line_item.order_item_key,
    line_item.part_key,
    line_item.line_number,
    line_item.extended_price,
    orders.order_key,
    orders.customer_key,
    orders.order_date,
    {{ discounted_amount('line_item.extended_price', 'line_item.discount_percentage') }} as item_discount_amount
from
    {{ ref('stg_tpch_orders') }} as orders
join
    {{ ref('stg_tpch_line_items') }} as line_item
        on orders.order_key = line_item.order_key
order by
    orders.order_date

```

Create `marts/int_order_items_summary.sql` to aggregate info

```sql
select 
    order_key,
    sum(extended_price) as gross_item_sales_amount,
    sum(item_discount_amount) as item_discount_amount
from
    {{ ref('int_order_items') }}
group by
    order_key

```

create fact model `models/marts/fct_orders.sql`

```sql
select
    orders.*,
    order_item_summary.gross_item_sales_amount,
    order_item_summary.item_discount_amount
from
    {{ref('stg_tpch_orders')}} as orders
join
    {{ref('int_order_items_summary')}} as order_item_summary
        on orders.order_key = order_item_summary.order_key
order by order_date

```

### Step 2.5: Creating Generic and Singular tests

Create `models/marts/generic_tests.yml`

```yaml
models:
  - name: fct_orders
    columns:
      - name: order_key
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('stg_tpch_orders')
              field: order_key
              severity: warn
      - name: status_code
        tests:
          - accepted_values:
              values: ['P', 'O', 'F']
```


Build Singular Tests `tests/fct_orders_discount.sql`

```sql
select
    *
from
    {{ref('fct_orders')}}
where
    item_discount_amount > 0
```

Create `tests/fct_orders_date_valid.sql`

```sql
select
    *
from
    {{ref('fct_orders')}}
where
    date(order_date) > CURRENT_DATE()
    or date(order_date) < date('1990-01-01')

```

### Step 3: Airflow Deployment

#### Step 3.1: Airflow setup

Update Dockerfile

```docker
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate
```

Update requirements.txt

```
astronomer-cosmos==1.12.0
dbt-snowflake==1.8.3
apache-airflow-providers-snowflake>=5.6.0
```

#### Step 3.2: Create `dbt_dag.py`
This project uses Astronomer Cosmos to render dbt projects as Airflow DAGs. The Airflow DAG automatically parses the dbt manifest and creates a task for every model, ensuring the stg_ models run before the fct_ models.

<img width="1027" height="390" alt="image" src="https://github.com/user-attachments/assets/a914b79c-906b-4ca6-a172-b13e51494ab6" />
