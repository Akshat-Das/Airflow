try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd
    import plotly.graph_objs as go
    import yfinance as yf
    import json
    import requests

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def first_function_execute():
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
    headers = {"Authorization": "Bearer ya29.a0AfH6SMDQqrn0TJDp88gSNxu1O7uiMoYFac7Jw8eh_PqNwR4HFcnschFv6tqI88FX3-aE-3fDNEa6sl1tkI8Yse4c90KMmn_Lf0hYecvvg5iPPxhpeeIXz1PqhWuZA1bddA8fER69Q9UyZn649gstul1QTK3A"}
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
        schedule_interval= '00 00 * * *',
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