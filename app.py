import argparse
import ast
from checker.checkers import CheckerFactory


def map_boolean_arguments(kwargs):
    for key, value in kwargs.items():
        if value.lower() == 'true' or value.lower() == 'false':
            kwargs[key] = ast.literal_eval(value.capitalize())

        try:
            kwargs[key] = int(value)
        except ValueError:
            continue
    return kwargs


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    # First must be Checker's name
    parser.add_argument("checker_name")

    # Admit any number of parameters after that
    parser.add_argument("kwargs", nargs="*")

    args = parser.parse_args()

    checker = CheckerFactory.get(package_name=args.package_name,
                                 module_name=args.module_name,
                                 class_name=args.checker_name)

    try:
        kwargs = dict(kwarg.split("=") for kwarg in args.kwargs)
    except ValueError:
        raise ValueError("Parameters must follow this format: name=value")

    kwargs = map_boolean_arguments(kwargs)

    checker.run(**kwargs)
