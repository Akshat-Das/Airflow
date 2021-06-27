try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    from airflow.models import Variable
    import pandas as pd
    import plotly.graph_objs as go
    import yfinance as yf
    import json
    import requests

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def first_function_execute():
    user = Variable.get("API_KEY")
    stock = yf.Ticker("INFY.NS")
    data = stock.history(period = '1d', interval = '1m', rounding= True)
    fig = go.Figure()
    fig.add_trace(go.Candlestick())
    fig.add_trace(go.Candlestick(x=data.index,open = data['Open'], high = data['High'], low = data['Low'], close = data['Close']))
    fig.update_layout(title = 'Infosys share price', yaxis_title = 'Stock Price (INR)')
    fig.show()
    fig.write_image("figure.png")
    para = {
        "name": "graph.png",
    }
    headers = {"Authorization": "Bearer " + user}
    files = {
        'data': ('metadata', json.dumps(para), 'application/json; charset=UTF-8'),
        'file': open("./figure.png", "rb")
    }
    r = requests.post(
        "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
        headers=headers,
        files=files
    )
    return r.text
with DAG(
        dag_id="first_dag",
        schedule_interval= '0 0 * * *',
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
        catchup=False) as f:

    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
    )