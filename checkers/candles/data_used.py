import pandas as pd

from copy import deepcopy
from datetime import datetime, timedelta
from typing import List

from env import EMAIL_FROM, DATA_USED_EMAIL_RECIPIENTS
from checker.connectors import ConnectorFactory
from checker.checkers.base import BaseChecker
from checker.models.candle import Candle
from checker.models.check import Check
from checker.utils import Email, EmailSender

from logger.logger import logger_checker



class CandleDataUsedChecker(BaseChecker):
    """
    Checks if there are candles with less ticks (data_used) than a given threshold.
    """
    name = "CandleDataUsedChecker"
    default_from_date = timedelta(weeks=1)
    default_to_date = timedelta(days=1)
    database_name = 'MySQL'
    resource_name = "Candle"
    th_number_ticks = 1
    email = Email(
        from_=EMAIL_FROM,
        to=", ".join(DATA_USED_EMAIL_RECIPIENTS),
        subject="Data Used In Candles Below Thresholds",
        body=(
            "If you get this message is because there is something wrong. "
            "There are Candles in DDBB created with few ticks. Summary:\n"
        )
    )

    def __init__(self):
        super().__init__()

        self.database = ConnectorFactory.get(name=self.database_name)

    def _run(self, from_date: str = None, to_date: str = None) -> Check:
        """
        For a given time intervale, checks if there are candles with less ticks than a permitted threshold.

        If there are candles with few ticks, and email is sent.

        :param from_date: start interval time
        :param to_date: end interval time
        :return: Check object with the result
        """
        from_date, to_date = self._get_date_interval(from_date=from_date, to_date=to_date)

        our_data = self._get_our_data(from_date=from_date, to_date=to_date)

        summary = self._check(our_data=our_data)

        if any(qty > 0 for qty in summary.values()):
            self._send_email(summary)

            logger_checker.info(f"{self.name}: Email sent")

            return Check(status="KO", message="There are Candles in DDBB created with few ticks")

        return Check(status="OK", message="")

    def _get_date_interval(self, from_date: str, to_date: str) -> [datetime, datetime]:
        """
        Checks the introduced date interval.

        If no argument is passed, default interval date is used.

        :param from_date: start interval datetime
        :param to_date: end interval datetime
        :return:
        """
        # One empty and one informed
        if bool(from_date) != bool(to_date):
            raise ValueError("El intervalo de fechas no está completo")

        today = datetime.utcnow()

        if from_date and to_date:
            from_date = datetime.strptime(from_date, self.date_format)
            to_date = datetime.strptime(to_date, self.date_format)
        else:
            from_date = today - self.default_from_date
            to_date = today - self.default_to_date

        logger_checker.debug(f'Time interval: {from_date} - {to_date}')
        return from_date, to_date

    def _get_our_data(self, from_date: datetime, to_date: datetime):
        """
        Gets candles from database for the time interval given.

        :param from_date: start interval datetime
        :param to_date: end interval datetime
        :return: formatted candles in a dataframe
        """
        filters = [(self.resource_name, "date_time", ">", from_date),
                   (self.resource_name, "date_time", "<", to_date)]

        result = self.database.query(models=[self.resource_name], filters=filters)

        data = self._format_our_data(data=result)

        logger_checker.debug(f'Our data acquired: {data}')
        return data

    def _format_our_data(self, data: List[Candle]) -> pd.DataFrame:
        """
        Formats candles data

        Converts to a pd.Dataframe, sets column types, drops duplicate entries...

        :param data: list of Candles
        :return: formatted candles in a dataframe
        """
        data = (
            pd.DataFrame(
                data=[candle.__dict__ for candle in data],
                columns=["id", "ticker_id", "date_time", "data_used"]
            )
            .astype({
                "id": "int",
                "ticker_id": "int",
                "date_time": "datetime64[ns]",
                "data_used": "int"
            })
            .drop_duplicates(["ticker_id", "date_time"])
            .set_index("date_time")
            .sort_index()
        )

        logger_checker.debug(f'Formated data: {data}')
        return data

    def _check(self, our_data: pd.DataFrame) -> dict:
        """
        Checks how many candles have data_used (ticks) < threshold

        :param our_data: candles in a dataframe
        :return: dictionary where keys are the thresholds and where values are the number of candles below that
        threshold
        """
        summary = {}

        data_below_threshold = our_data.loc[our_data["data_used"] < CandleDataUsedChecker.th_number_ticks]

        summary[CandleDataUsedChecker.th_number_ticks] = len(data_below_threshold.index)

        return summary

    def _send_email(self, summary: dict) -> None:
        """
        Builds the email and sends it to the given recipients

        :param summary: dictionary that contains a set of threshold values and the corresponding number of
        candles below each threshold
        :return:
        """

        email = deepcopy(self.email)

        today = datetime.utcnow().strftime('%d-%m-%Y')

        email.subject = email.subject + f" {today}"

        summary_as_str = '\n'.join([f'Candles below {threshold}: {qty}'
                                    for threshold, qty in summary.items()])

        email.body += summary_as_str

        EmailSender.send_email(email=email)
        logger_checker.debug(f'Email sent to: {email}')
