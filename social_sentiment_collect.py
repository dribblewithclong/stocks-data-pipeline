"""
daily: to_time = from_time + 1 day
"""

import json
import finnhub
import numpy as np


class SocialSentimentCollector:

    def __init__(self, stock_symbol, from_time, to_time):
        self.from_time = from_time
        self.to_time = to_time
        self.stock_symbol = stock_symbol

        with open('finhub_api.json', 'r') as api:
            self.api_key = json.load(api)['api_key']
        self.client = finnhub.Client(
            self.api_key
        )

    def get_reddit_sentiment(self):
        reddit_sentiment = self.client.stock_social_sentiment(
            symbol=self.stock_symbol,
            _from=self.from_time,
            to=self.to_time,
        )['reddit']

        return reddit_sentiment

    def get_twitter_sentiment(self):
        twitter_sentiment = self.client.stock_social_sentiment(
            symbol=self.stock_symbol,
            _from=self.from_time,
            to=self.to_time,
        )['twitter']

        return twitter_sentiment

    def get_reddit_total_mentions(self):
        reddit_sentiment = self.get_reddit_sentiment()

        if len(reddit_sentiment) == 0:
            return 0

        number_mentions = np.vectorize(
            lambda x: x['mention'],
        )
        total_mentions = np.sum(
            number_mentions(
                reddit_sentiment
            )
        )
        return total_mentions

    def get_twitter_total_mentions(self):
        twitter_sentiment = self.get_twitter_sentiment()

        if len(twitter_sentiment) == 0:
            return 0

        number_mentions = np.vectorize(
            lambda x: x['mention'],
        )
        total_mentions = np.sum(
            number_mentions(
                twitter_sentiment
            )
        )

        return total_mentions

    def get_reddit_total_positive_mentions(self):
        reddit_sentiment = self.get_reddit_sentiment()

        if len(reddit_sentiment) == 0:
            return 0

        number_mentions = np.vectorize(
            lambda x: x['positiveMention'],
        )
        total_mentions = np.sum(
            number_mentions(
                reddit_sentiment
            )
        )

        return total_mentions

    def get_twitter_total_positive_mentions(self):
        twitter_sentiment = self.get_twitter_sentiment()

        if len(twitter_sentiment) == 0:
            return 0

        number_mentions = np.vectorize(
            lambda x: x['positiveMention'],
        )
        total_mentions = np.sum(
            number_mentions(
                twitter_sentiment
            )
        )

        return total_mentions

    def get_reddit_total_negative_mentions(self):
        reddit_sentiment = self.get_reddit_sentiment()

        if len(reddit_sentiment) == 0:
            return 0

        number_mentions = np.vectorize(
            lambda x: x['negativeMention'],
        )
        total_mentions = np.sum(
            number_mentions(
                reddit_sentiment
            )
        )

        return total_mentions

    def get_twitter_total_negative_mentions(self):
        twitter_sentiment = self.get_twitter_sentiment()

        if len(twitter_sentiment) == 0:
            return 0

        number_mentions = np.vectorize(
            lambda x: x['negativeMention'],
        )
        total_mentions = np.sum(
            number_mentions(
                twitter_sentiment
            )
        )

        return total_mentions

    def get_avg_reddit_sentiment_score(self):
        reddit_sentiment = self.get_reddit_sentiment()

        if len(reddit_sentiment) == 0:
            return 0

        sentiment_scores = np.vectorize(
            lambda x: x['score'],
        )
        avg_sentiment_score = np.mean(
            sentiment_scores(
                reddit_sentiment
            )
        )

        return avg_sentiment_score

    def get_avg_twitter_sentiment_score(self):
        twitter_sentiment = self.get_twitter_sentiment()

        if len(twitter_sentiment) == 0:
            return 0

        sentiment_scores = np.vectorize(
            lambda x: x['score'],
        )
        avg_sentiment_score = np.mean(
            sentiment_scores(
                twitter_sentiment
            )
        )

        return avg_sentiment_score
