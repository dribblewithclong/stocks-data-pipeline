import datetime as dt
import json

import polars as pl
import psycopg2
from psycopg2 import sql, extras

from stock_articles_collect import ArticleCollector
from social_sentiment_collect import SocialSentimentCollector
from stock_price_collect import StockPriceCollector
from stock_info_collect import StockInfoCollector


class StockInfoIngestion:

    def __init__(self):
        self.stock_symbols = [
            'META',
            'AMZN',
            'AAPL',
            'NFLX',
            'GOOGL',
            'TSLA',
            'MSFT',
        ]

        self.table_id = 'stock_info'

    def extract_data(self):
        all_stocks_data = list()

        # Loop through the stock symbols to extract data of them
        for symbol in self.stock_symbols:
            collector = StockInfoCollector(symbol)
            stock = collector.get_stock_info()
            all_stocks_data.append(stock)

        return all_stocks_data

    def transform_data(self, raw_data):
        df = pl.DataFrame(raw_data)

        # Rename and reorder columns of dataframe
        df = df.rename(
            {
                'ticker': 'symbol',
                'estimateCurrency': 'estimate_currency',
                'finnhubIndustry': 'industry',
                'ipo': 'ipo_date',
                'marketCapitalization': 'market_capitalization',
                'shareOutstanding': 'share_outstanding',
                'weburl': 'web_url',
            }
        ).select(
            [
                'symbol',
                'country',
                'currency',
                'estimate_currency',
                'exchange',
                'industry',
                'ipo_date',
                'logo',
                'market_capitalization',
                'name',
                'phone',
                'share_outstanding',
                'web_url',
            ]
        )

        # Convert data type of column to datetime format
        df = df.with_columns(
            df['ipo_date'].apply(
                lambda x: dt.datetime.strptime(x, '%Y-%m-%d')
            )
        )

        return df

    def load_data(self, transformed_data):
        # Create sql identifiers for the column names
        columns = sql.SQL(',').join(
            sql.Identifier(name) for name in transformed_data.columns
        )
        # Create placeholders for the values
        values = sql.SQL(',').join(
            [
                sql.Placeholder() for _ in transformed_data.columns
            ]
        )

        # Prepare the insert query
        insert_query = sql.SQL(
            'INSERT INTO {} ({}) VALUES({}) ON CONFLICT DO NOTHING;'
        ).format(
            sql.Identifier(self.table_id), columns, values
        )

        # Get config for the database
        with open('postgresql.json', 'r') as psql:
            config = json.load(psql)
        host = config['host']
        db = config['database']
        user = config['user']
        password = config['password']
        port = config['port']

        # Make a connection
        conn = psycopg2.connect(
            host=host,
            database=db,
            user=user,
            password=password,
            port=port,
        )
        cur = conn.cursor()

        # Load data to the database
        extras.execute_batch(
            cur,
            insert_query,
            transformed_data.rows(),
        )

        cur.close()
        conn.commit()

    def main(self):
        raw_data = self.extract_data()
        transformed_data = self.transform_data(raw_data)
        self.load_data(transformed_data)


class StockPriceIngest:
    def __init__(self, stock_symbol, from_time, to_time):
        self.stock_symbol = stock_symbol
        self.from_time = from_time
        self.to_time = to_time

        self.table_id = 'stock_price'

    def extract_data(self):
        collector = StockPriceCollector(
            self.stock_symbol,
            self.from_time,
            self.to_time,
        )

        price = collector.get_price()
        volume = collector.get_volume()

        return {
            'price': price,
            'volume': volume,
        }

    def transform_data(self, raw_data):
        # Extract value of each columns from raw data
        price_col = list(raw_data['price'].values())
        volume_col = list(raw_data['volume'].values())
        datetime_col = list(raw_data['price'].keys())

        if len(price_col) == 0:
            return False

        df = pl.DataFrame([datetime_col, price_col, volume_col])

        # Rename columns
        df.columns = ['date', 'price', 'volume']

        # Create new columns
        df = df.with_columns(
            pl.lit(self.stock_symbol).alias('symbol')
        )
        df = df.with_columns(
            (
                df['symbol'] + '_' + df['date']
            ).alias('id')
        )

        # Reorder columns and sort by id
        df = df.select(
            [
                'id',
                'symbol',
                'date',
                'price',
                'volume',
            ]
        ).sort('id')

        return df

    def load_data(self, transformed_data):
        if transformed_data is False:
            return

        # Create sql identifiers for the column names
        columns = sql.SQL(',').join(
            sql.Identifier(name) for name in transformed_data.columns
        )
        # Create placeholders for the values
        values = sql.SQL(',').join(
            [
                sql.Placeholder() for _ in transformed_data.columns
            ]
        )

        # Prepare the insert query
        insert_query = sql.SQL(
            'INSERT INTO {} ({}) VALUES({}) ON CONFLICT DO NOTHING;'
        ).format(
            sql.Identifier(self.table_id), columns, values
        )

        # Get config for the database
        with open('postgresql.json', 'r') as psql:
            config = json.load(psql)
        host = config['host']
        db = config['database']
        user = config['user']
        password = config['password']
        port = config['port']

        # Make a connection
        conn = psycopg2.connect(
            host=host,
            database=db,
            user=user,
            password=password,
            port=port,
        )
        cur = conn.cursor()

        # Load data to the database
        extras.execute_batch(
            cur,
            insert_query,
            transformed_data.rows(),
        )

        cur.close()
        conn.commit()

    def main(self):
        raw_data = self.extract_data()
        transformed_data = self.transform_data(raw_data)
        self.load_data(transformed_data)


class StockRedditIngest:

    def __init__(self, stock_symbol, from_time, to_time):
        self.stock_symbol = stock_symbol
        self.from_time = from_time
        self.to_time = to_time

        self.table_id = 'stock_reddit_sentiment'

    def extract_data(self):
        collector = SocialSentimentCollector(
            self.stock_symbol,
            self.from_time,
            self.to_time,
        )

        reddit_sentiment = collector.get_reddit_sentiment()

        return reddit_sentiment

    def transform_data(self, raw_data):
        if len(raw_data) == 0:
            return False

        df = pl.DataFrame(raw_data)

        # Rename columns
        df = df.rename(
            {
                'positiveScore': 'positive_score',
                'negativeScore': 'negative_score',
                'positiveMention': 'positive_mention',
                'negativeMention': 'negative_mention',
                'score': 'sentiment_score',
                'atTime': 'date',

            }
        )

        # Create new columns
        df = df.with_columns(
            pl.lit(self.stock_symbol).alias('symbol')
        )
        df = df.with_columns(
            df['date'].apply(
                lambda x: x.split(' ')[1]
            ).alias('time')
        )
        df = df.with_columns(
            (
                'reddit' + '_'
                + df['symbol'] + '_'
                + df['date'].apply(lambda x: x.split(' ')[0])
                + '_' + df['time']
            ).alias('id')
        )
        df = df.with_columns(
            df['date'].apply(
                lambda x: dt.datetime.strptime(
                    x.split(' ')[0],
                    '%Y-%m-%d',
                )
            )
        )

        # Reorder columns and sort by id
        df = df.select(
            [
                'id',
                'symbol',
                'date',
                'time',
                'mention',
                'positive_score',
                'negative_score',
                'positive_mention',
                'negative_mention',
                'sentiment_score',
            ]
        ).sort('id')

        return df

    def load_data(self, transformed_data):
        if transformed_data is False:
            return

        # Create sql identifiers for the column names
        columns = sql.SQL(',').join(
            sql.Identifier(name) for name in transformed_data.columns
        )
        # Create placeholders for the values
        values = sql.SQL(',').join(
            [
                sql.Placeholder() for _ in transformed_data.columns
            ]
        )

        # Prepare the insert query
        insert_query = sql.SQL(
            'INSERT INTO {} ({}) VALUES({}) ON CONFLICT DO NOTHING;'
        ).format(
            sql.Identifier(self.table_id), columns, values
        )

        # Get config for the database
        with open('postgresql.json', 'r') as psql:
            config = json.load(psql)
        host = config['host']
        db = config['database']
        user = config['user']
        password = config['password']
        port = config['port']

        # Make a connection
        conn = psycopg2.connect(
            host=host,
            database=db,
            user=user,
            password=password,
            port=port,
        )
        cur = conn.cursor()

        # Load data to the database
        extras.execute_batch(
            cur,
            insert_query,
            transformed_data.rows(),
        )

        cur.close()
        conn.commit()

    def main(self):
        raw_data = self.extract_data()
        transformed_data = self.transform_data(raw_data)
        self.load_data(transformed_data)


class StockTwitterIngest:

    def __init__(self, stock_symbol, from_time, to_time):
        self.stock_symbol = stock_symbol
        self.from_time = from_time
        self.to_time = to_time

        self.table_id = 'stock_twitter_sentiment'

    def extract_data(self):
        collector = SocialSentimentCollector(
            self.stock_symbol,
            self.from_time,
            self.to_time,
        )

        twitter_sentiment = collector.get_twitter_sentiment()

        return twitter_sentiment

    def transform_data(self, raw_data):
        if len(raw_data) == 0:
            return False

        df = pl.DataFrame(raw_data)

        # Rename columns
        df = df.rename(
            {
                'positiveScore': 'positive_score',
                'negativeScore': 'negative_score',
                'positiveMention': 'positive_mention',
                'negativeMention': 'negative_mention',
                'score': 'sentiment_score',
                'atTime': 'date',

            }
        )

        # Create new columns
        df = df.with_columns(
            pl.lit(self.stock_symbol).alias('symbol')
        )
        df = df.with_columns(
            df['date'].apply(
                lambda x: x.split(' ')[1]
            ).alias('time')
        )
        df = df.with_columns(
            (
                'twitter' + '_'
                + df['symbol'] + '_'
                + df['date'].apply(lambda x: x.split(' ')[0])
                + '_' + df['time']
            ).alias('id')
        )
        df = df.with_columns(
            df['date'].apply(
                lambda x: dt.datetime.strptime(
                    x.split(' ')[0],
                    '%Y-%m-%d',
                )
            )
        )

        # Reorder columns and sort by id
        df = df.select(
            [
                'id',
                'symbol',
                'date',
                'time',
                'mention',
                'positive_score',
                'negative_score',
                'positive_mention',
                'negative_mention',
                'sentiment_score'
            ]
        ).sort('id')

        return df

    def load_data(self, transformed_data):
        if transformed_data is False:
            return

        # Create sql identifiers for the column names
        columns = sql.SQL(',').join(
            sql.Identifier(name) for name in transformed_data.columns
        )
        # Create placeholders for the values
        values = sql.SQL(',').join(
            [
                sql.Placeholder() for _ in transformed_data.columns
            ]
        )

        # Prepare the insert query
        insert_query = sql.SQL(
            'INSERT INTO {} ({}) VALUES({}) ON CONFLICT DO NOTHING;'
        ).format(
            sql.Identifier(self.table_id), columns, values
        )

        # Get config for the database
        with open('postgresql.json', 'r') as psql:
            config = json.load(psql)
        host = config['host']
        db = config['database']
        user = config['user']
        password = config['password']
        port = config['port']

        # Make a connection
        conn = psycopg2.connect(
            host=host,
            database=db,
            user=user,
            password=password,
            port=port,
        )
        cur = conn.cursor()

        # Load data to the database
        extras.execute_batch(
            cur,
            insert_query,
            transformed_data.rows(),
        )

        cur.close()
        conn.commit()

    def main(self):
        raw_data = self.extract_data()
        transformed_data = self.transform_data(raw_data)
        self.load_data(transformed_data)


class StockArticleIngest:

    def __init__(self, stock_symbol, from_time, to_time):
        self.stock_symbol = stock_symbol
        self.from_time = from_time
        self.to_time = to_time

        self.table_id = 'stock_articles'

    def extract_data(self):
        collector = ArticleCollector(
            self.stock_symbol,
            self.from_time,
            self.to_time,
        )

        articles = collector.get_articles()

        return articles

    def transform_data(self, raw_data):
        if len(raw_data) == 0:
            return False

        df = pl.DataFrame(raw_data)

        # Rename columns
        df = df.rename(
            {
                'related': 'symbol',
                'image': 'image_url',
                'url': 'article_url',
                'datetime': 'date'
            }
        )

        # Create new columns
        df = df.with_columns(
            df['date'].apply(
                lambda x: dt.datetime.utcfromtimestamp(
                    int(x)
                ).strftime('%H:%M:%S')
            ).alias('time')
        )
        df = df.with_columns(
            df['date'].apply(
                lambda x: dt.datetime.utcfromtimestamp(
                    int(x)
                ).replace(hour=0, minute=0, second=0)
            )
        )
        df = df.with_columns(
            df['id'].apply(
                lambda x: self.stock_symbol + '_' + str(x)
            )
        )

        # Reorder columns and sort by id
        df = df.select(
            [
                'id',
                'symbol',
                'date',
                'time',
                'category',
                'headline',
                'image_url',
                'source',
                'summary',
                'article_url'
            ]
        ).sort('id')

        return df

    def load_data(self, transformed_data):
        if transformed_data is False:
            return

        # Create sql identifiers for the column names
        columns = sql.SQL(',').join(
            sql.Identifier(name) for name in transformed_data.columns
        )
        # Create placeholders for the values
        values = sql.SQL(',').join(
            [
                sql.Placeholder() for _ in transformed_data.columns
            ]
        )

        # Prepare the insert query
        insert_query = sql.SQL(
            'INSERT INTO {} ({}) VALUES({}) ON CONFLICT DO NOTHING;'
        ).format(
            sql.Identifier(self.table_id), columns, values
        )

        # Get config for the database
        with open('postgresql.json', 'r') as psql:
            config = json.load(psql)
        host = config['host']
        db = config['database']
        user = config['user']
        password = config['password']
        port = config['port']

        # Make a connection
        conn = psycopg2.connect(
            host=host,
            database=db,
            user=user,
            password=password,
            port=port,
        )
        cur = conn.cursor()

        # Load data to the database
        extras.execute_batch(
            cur,
            insert_query,
            transformed_data.rows(),
        )

        cur.close()
        conn.commit()

    def main(self):
        raw_data = self.extract_data()
        transformed_data = self.transform_data(raw_data)
        self.load_data(transformed_data)


if __name__ == '__main__':
    StockInfoIngestion().main()

    from_time = '2022-03-31'
    to_time = '2022-04-01'

    for symbol in [
        'META',
        'AMZN',
        'AAPL',
        'NFLX',
        'GOOGL',
        'TSLA',
        'MSFT',
    ]:
        StockArticleIngest(
            symbol,
            from_time,
            from_time,
        ).main()
        to_time = '2022-04-01'
        StockPriceIngest(
            symbol,
            from_time,
            to_time,
        ).main()
        StockRedditIngest(
            symbol,
            from_time,
            to_time,
        ).main()
        StockTwitterIngest(
            symbol,
            from_time,
            to_time,
        ).main()

        print(f'Done ingest data for {symbol}')
