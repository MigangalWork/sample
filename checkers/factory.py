from importlib import import_module
from logger.logger import logger_factory

class CheckerFactory:
    """
    Checker's factory.

    Every checker must be imported in this script to correctly create an instance the desired checker.
    """
    @staticmethod
    def get(package_name: str, module_name: str, class_name: str):
        """
        Generates an instance of a checker given its name.

        :param name: Checker's name (class name)
        :return: instance of the checker
        """

        try:
            imported_module = import_module(f'{package_name}.{module_name}')
            logger_factory.debug(f'Imported module: {imported_module}')

            module_class = getattr(imported_module, class_name)
            logger_factory.info(f'Imported class: {module_class}')

            return module_class()

        except Exception as e:
            logger_factory.error("Unable to get the checker")
            logger_factory.error(str(e))
