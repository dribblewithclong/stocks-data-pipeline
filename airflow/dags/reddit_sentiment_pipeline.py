import datetime as dt

from airflow.decorators import dag, task

from data_ingest import StockRedditIngest


@dag(
    dag_id='reddit_sentiment_ingestion',
    schedule_interval='0 2 * * *',
    start_date=dt.datetime(2022, 4, 1, 0, 0, 0),
    max_active_runs=2,
    concurrency=2,
    catchup=True,
)
def reddit_sentiment_ingest():

    def retrieve_time(**kwargs):
        from_time = (
            kwargs["data_interval_start"]
            - dt.timedelta(hours=2)
        ).strftime('%Y-%m-%d')
        to_time = (
            kwargs["data_interval_end"]
            - dt.timedelta(hours=2)
        ).strftime('%Y-%m-%d')

        return from_time, to_time

    for symbol in [
        'META',
        'AMZN',
        'AAPL',
        'NFLX',
        'GOOGL',
        'TSLA',
        'MSFT',
    ]:
        @task(
            task_id=f'extract_data_{symbol}',
            retries=3,
        )
        def extract(symbol, **kwargs):
            from_time, to_time = retrieve_time(**kwargs)

            raw_data = StockRedditIngest(
                symbol,
                from_time,
                to_time,
            ).extract_data()

            return raw_data

        @task(
            task_id=f'transform_data_{symbol}',
            multiple_outputs=True,
            retries=3,
        )
        def transform(symbol, raw_data, **kwargs):
            from_time, to_time = retrieve_time(**kwargs)

            transformed_data = StockRedditIngest(
                symbol,
                from_time,
                to_time,
            ).transform_data(raw_data)

            return transformed_data

        @task(
            task_id=f'load_data_{symbol}',
            retries=3,
        )
        def load(symbol, transformed_data, **kwargs):
            from_time, to_time = retrieve_time(**kwargs)

            StockRedditIngest(
                symbol,
                from_time,
                to_time,
            ).load_data(transformed_data)

        raw_data = extract(symbol)
        transformed_data = transform(symbol, raw_data)
        load(symbol, transformed_data)


reddit_sentiment_ingest()
