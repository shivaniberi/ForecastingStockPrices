# In Cloud Composer, add snowflake-connector-python to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from datetime import datetime
import snowflake.connector


def return_snowflake_conn():
    user_id = Variable.get('snowflake_userid')
    password = Variable.get('snowflake_password')
    account = Variable.get('snowflake_account')

    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,  # Example: 'xyz12345.us-east-1'
        warehouse='compute_wh',
        database='demoapi'  # Use demoapi database
    )
    return conn


@task
def train(train_input_table, train_view, forecast_function_name):
    """
    Create a view with training related columns and create a model with the view above.
    """
    conn = return_snowflake_conn()
    cur = conn.cursor()
    
    # Filter to include both MSFT and NVDA in the training view
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        DATE, CLOSE, SYMBOL
        FROM {train_input_table}
        WHERE SYMBOL IN ('MSFT', 'NVDA');"""

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
        # Inspect the accuracy metrics of your model. 
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(f"Error during training: {e}")
        raise
    finally:
        cur.close()
        conn.close()


@task
def predict(forecast_function_name, train_input_table, forecast_table, final_table):
    """
    Generate predictions and store the results to a table named forecast_table.
    Union your predictions with your historical data, then create the final table.
    """
    conn = return_snowflake_conn()
    cur = conn.cursor()

    make_prediction_sql = f"""BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""
    
    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        WHERE SYMBOL IN ('MSFT', 'NVDA')  -- Include both symbols
        UNION ALL
        SELECT REPLACE(series, '"', '') AS SYMBOL, ts AS DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(f"Error during prediction: {e}")
        raise
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id='TrainPredict',
    start_date=datetime(2024, 9, 21),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule='30 2 * * *'
) as dag:

    # Updated variables with specified table names
    train_input_table = "demoapi.raw_data.stock_price"  # Table with stock price data
    train_view = "demoapi.adhoc.market_data_view"        # View for training data
    forecast_table = "demoapi.adhoc.market_data_forecast" # Table for storing forecast results
    forecast_function_name = "demoapi.analytics.predict_stock_price" # Forecast function name
    final_table = "demoapi.analytics.market_data" # Final table for results

    # Define task dependencies
    train_task = train(train_input_table, train_view, forecast_function_name)
    predict_task = predict(forecast_function_name, train_input_table, forecast_table, final_table)

    # Set the dependency: train runs before predict
    train_task >> predict_task
