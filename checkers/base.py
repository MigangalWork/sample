from datetime import datetime
import logging
import io

from checker.connectors import ConnectorFactory
from checker.models.check import Check

from logger.logger import logger_factory


class BaseChecker:
    """
    Main Checker class

    Implements the main method run() and manages common checker features like inserting a log to the database after
    a checker has been executed.

    Every Checker must inherit from this class.

    """
    name: str

    def __init__(self):
        """
        Inits logger, connection to the database and checks if the checker called exits in the database
        """
        self.meta_database = ConnectorFactory.get(name='MySQL')

        self.checker_type = self._get_checker_type()

    def run(self, *args, **kwargs):
        """
        Main method of every checker.

        This is the method that has to be called to execute a checker.

        First, it checks that the checker is active (active = 1 in sauron.aux_checker_types). If it isn't active,
        the checker won't be executed and the method will be exited.

        If the checker is active, the method _run() will be called and the core of the checker will be executed.
        Finally, a log will be inserted to the database (sauron.checks) with the result of the execution (OK/KO) and
        a brief message.

        :param args:
        :param kwargs:
        :return:
        """
        if not self.checker_type.active:
            return

        logger_factory.info(f"{self.name}: Check started")

        check = self._run(*args, **kwargs)

        print(f"{self.name}: Check result -> {check.status} {check.message}")

        self._write_check_in_ddbb(check)

        logger_factory.info(f"{self.name}: Check finished")

    def _get_checker_type(self):
        """
        Gets the corresponding row from the database of to the checker that wants to be executed.

        It must exist in the database, or it will crash.

        :return:
        """
        filters = [("CheckerType", "name", "==", self.name)]
        checker_type = self.meta_database.query(models=["CheckerType"], filters=filters)[0]

        return checker_type

    def _run(self, *args, **kwargs):
        """
        Main method that implements the functionality of the checker

        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def _write_check_in_ddbb(self, check: Check):
        """
        Inserts a log with the result of the execution to the database (sauron.checks)

        :param check:
        :return:
        """
        check.checker_type_id = self.checker_type.id
        check.checked_at = datetime.utcnow()

        self.meta_database.orm_insert(models=[check])
