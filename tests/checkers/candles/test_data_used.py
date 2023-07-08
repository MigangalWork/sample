import numpy as np
import pandas as pd
import pytest

import datetime
from checker.connectors.factory import ConnectorFactory
from checker.checkers.candles.data_used import CandleDataUsedChecker
from tests.connectors.test_mysql import MySQLMock


class TestCandleDataUsedChecker:

    @pytest.mark.parametrize("our_data, expected", [
        # Regular case, no data_used below any threshold
        (
            pd.DataFrame(
                data=[
                    {"id": 1, "ticker_id": 1, "data_used": 1200, "mark": np.nan,
                     "open": 1.42, "close": 1.44, "high": 1.52, "low": 1.33},
                    {"id": 2, "ticker_id": 1, "data_used": 1200, "mark": np.nan,
                     "open": 1.43, "close": 1.45, "high": 1.53, "low": 1.34},
                    {"id": 3, "ticker_id": 1, "data_used": 1200, "mark": np.nan,
                     "open": 1.44, "close": 1.46, "high": 1.54, "low": 1.35},
                    {"id": 4, "ticker_id": 1, "data_used": 1200, "mark": np.nan,
                     "open": 1.45, "close": 1.47, "high": 1.55, "low": 1.36},
                    {"id": 5, "ticker_id": 1, "data_used": 1200, "mark": np.nan,
                     "open": 1.46, "close": 1.48, "high": 1.56, "low": 1.37},
                ],
                index=[
                    datetime.datetime.strptime("2022-07-06T16:00:00Z", '%Y-%m-%dT%H:%M:%SZ'),
                    datetime.datetime.strptime("2022-07-06T16:01:00Z", '%Y-%m-%dT%H:%M:%SZ'),
                    datetime.datetime.strptime("2022-07-06T16:02:00Z", '%Y-%m-%dT%H:%M:%SZ'),
                    datetime.datetime.strptime("2022-07-06T16:03:00Z", '%Y-%m-%dT%H:%M:%SZ'),
                    datetime.datetime.strptime("2022-07-06T16:04:00Z", '%Y-%m-%dT%H:%M:%SZ')
                ],
                columns=["id", "ticker_id", "data_used", "mark", "open", "close", "high", "low"]
            ).rename_axis("date_time"),
            {
                1150: 0,
            }
        ),
        # Multiple data_used below thresholds
        (
            pd.DataFrame(
                data=[
                    {"id": 1, "ticker_id": 1, "data_used": 1200, "mark": np.nan,
                     "open": 1.42, "close": 1.44, "high": 1.52, "low": 1.33},
                    {"id": 2, "ticker_id": 1, "data_used": 600, "mark": np.nan,
                     "open": 1.43, "close": 1.45, "high": 1.53, "low": 1.34},
                    {"id": 3, "ticker_id": 1, "data_used": 900, "mark": np.nan,
                     "open": 1.44, "close": 1.46, "high": 1.54, "low": 1.35},
                    {"id": 4, "ticker_id": 1, "data_used": 1200, "mark": np.nan,
                     "open": 1.45, "close": 1.47, "high": 1.55, "low": 1.36},
                    {"id": 5, "ticker_id": 1, "data_used": 1149, "mark": np.nan,
                     "open": 1.46, "close": 1.48, "high": 1.56, "low": 1.37},
                    {"id": 6, "ticker_id": 1, "data_used": 1200, "mark": np.nan,
                     "open": 1.46, "close": 1.48, "high": 1.56, "low": 1.37},
                    {"id": 7, "ticker_id": 1, "data_used": 600, "mark": np.nan,
                     "open": 1.46, "close": 1.48, "high": 1.56, "low": 1.37},
                    {"id": 8, "ticker_id": 1, "data_used": 1200, "mark": np.nan,
                     "open": 1.46, "close": 1.48, "high": 1.56, "low": 1.37},
                ],
                index=[
                    datetime.datetime.strptime("2022-07-06T16:00:00Z", '%Y-%m-%dT%H:%M:%SZ'),
                    datetime.datetime.strptime("2022-07-06T16:01:00Z", '%Y-%m-%dT%H:%M:%SZ'),
                    datetime.datetime.strptime("2022-07-06T16:02:00Z", '%Y-%m-%dT%H:%M:%SZ'),
                    datetime.datetime.strptime("2022-07-06T16:03:00Z", '%Y-%m-%dT%H:%M:%SZ'),
                    datetime.datetime.strptime("2022-07-06T16:04:00Z", '%Y-%m-%dT%H:%M:%SZ'),
                    datetime.datetime.strptime("2022-07-06T16:05:00Z", '%Y-%m-%dT%H:%M:%SZ'),
                    datetime.datetime.strptime("2022-07-06T16:06:00Z", '%Y-%m-%dT%H:%M:%SZ'),
                    datetime.datetime.strptime("2022-07-06T16:07:00Z", '%Y-%m-%dT%H:%M:%SZ')
                ],
                columns=["id", "ticker_id", "data_used", "mark", "open", "close", "high", "low"]
            ).rename_axis("date_time"),
            {
                1150: 4,
            }
        ),
    ])
    def test_check(self, our_data, expected):

        summary = CandleDataUsedChecker()._check(our_data)

        assert summary == expected

    @pytest.mark.parametrize("from_date, to_date, previous_data, expected", [
        (
            "2022-08-12T00:00:00Z",
            "2022-08-17T00:00:00Z",
            {
                "CheckerType": [
                    {"name": CandleDataUsedChecker.name, "active": 1}
                ],
                "Check": [],
                CandleDataUsedChecker.resource_name: [
                    {"ticker_id": 1, "data_used": 1200, "mark": None, "date_time": "2022-08-11 16:00:00",
                     "open": 1.42, "close": 1.44, "high": 1.52, "low": 1.33},
                    {"ticker_id": 1, "data_used": 1200, "mark": None, "date_time": "2022-08-12 16:01:00",
                     "open": 1.43, "close": 1.45, "high": 1.53, "low": 1.34},
                    {"ticker_id": 1, "data_used": 1151, "mark": None, "date_time": "2022-08-13 16:02:00",
                     "open": 1.44, "close": 1.46, "high": 1.54, "low": 1.35},
                    {"ticker_id": 1, "data_used": 1150, "mark": None, "date_time": "2022-08-14 16:03:00",
                     "open": 1.45, "close": 1.47, "high": 1.55, "low": 1.36},
                    {"ticker_id": 1, "data_used": 1200, "mark": None, "date_time": "2022-08-15 16:04:00",
                     "open": 1.46, "close": 1.48, "high": 1.56, "low": 1.37}
                ]
            },
            {
                "Check": [
                    {"checker_type_id": 1, "status": "OK", "message": ""}
                ]
            }
        ),
        (
            "2022-08-12T00:00:00Z",
            "2022-08-17T00:00:00Z",
            {
                "CheckerType": [
                    {"name": CandleDataUsedChecker.name, "active": 1}],
                "Check": [],
                CandleDataUsedChecker.resource_name: [
                    {"ticker_id": 1, "data_used": 900, "mark": None, "date_time": "2022-08-11 16:00:00",
                     "open": 1.42, "close": 1.44, "high": 1.52, "low": 1.33},
                    {"ticker_id": 1, "data_used": 900, "mark": None, "date_time": "2022-08-12 16:01:00",
                     "open": 1.43, "close": 1.45, "high": 1.53, "low": 1.34},
                    {"ticker_id": 1, "data_used": 1149, "mark": None, "date_time": "2022-08-13 16:02:00",
                     "open": 1.44, "close": 1.46, "high": 1.54, "low": 1.35},
                    {"ticker_id": 1, "data_used": 1150, "mark": None, "date_time": "2022-08-14 16:03:00",
                     "open": 1.45, "close": 1.47, "high": 1.55, "low": 1.36},
                    {"ticker_id": 1, "data_used": 1200, "mark": None, "date_time": "2022-08-15 16:04:00",
                     "open": 1.46, "close": 1.48, "high": 1.56, "low": 1.37}]
            },
            {
                "Check": [
                    {"checker_type_id": 1, "status": "KO",
                     "message": "There are Candles in DDBB created with few ticks"}]
            }
        )
    ])
    def test_run(self, from_date, to_date, previous_data, expected, mocker):

        with MySQLMock(previous_data.keys()) as mysql:
            mocker.patch.object(ConnectorFactory, "get", return_value=mysql)

            for resource_name, data in previous_data.items():
                if data:
                    mysql.core_upsert(resource_name, data)

            CandleDataUsedChecker().run(from_date, to_date)

            for resource_name, expected_data in expected.items():

                columns = ",".join(expected_data[0].keys())

                table = mysql.tables[resource_name]

                query = f"SELECT {columns} FROM {table.schema}.{table.name}"

                result = [dict(zip(r.keys(), r)) for r in mysql.execute(resource_name, query)]

                assert result == expected_data
