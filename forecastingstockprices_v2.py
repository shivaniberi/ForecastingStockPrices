# In Cloud Composer, add snowflake-connector-python to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from datetime import timedelta, datetime
import snowflake.connector

def return_snowflake_conn():
    user_id = Variable.get('snowflake_userid')
    password = Variable.get('snowflake_password')
    account = Variable.get('snowflake_account')

    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,
        warehouse='compute_wh',
        database='DEMOAPI'
    )
    # Create a cursor object
    return conn.cursor()

@task
def train(cur, train_input_table, train_view, forecast_function_name):
    """
    - Create a view with training related columns
    - Create a model with the view above
    """
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        DATE, CLOSE, SYMBOL
        FROM {train_input_table};"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        # Inspect the accuracy metrics of your model
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(e)
        raise

@task
def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    """
    - Generate predictions and store the results to a table named forecast_table.
    - Union your predictions with your historical data, then create the final table
    """
    make_prediction_sql = f"""BEGIN
        -- This step creates your predictions.
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            -- Here we set your prediction interval.
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        -- These steps store your predictions to a table.
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""

    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT REPLACE(series, '"', '') AS SYMBOL, ts AS DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise

with DAG(
    dag_id='TrainPredict',
    start_date=datetime(2024, 9, 21),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule='30 2 * * *'  # Adjust schedule as needed
) as dag:

    train_input_table = "demoapi.raw_data.stock_price"
    train_view = "demoapi.adhoc.market_data_view"
    forecast_table = "demoapi.adhoc.market_data_forecast"
    forecast_function_name = "demoapi.analytics.predict_stock_price"
    final_table = "demoapi.analytics.market_data"

    # Initialize Snowflake connection
    cur = return_snowflake_conn()

    # Define task dependencies
    training_task = train(cur, train_input_table, train_view, forecast_function_name)
    prediction_task = predict(cur, forecast_function_name, train_input_table, forecast_table, final_table)

    # Chain the tasks: predict after train
    training_task >> prediction_task
