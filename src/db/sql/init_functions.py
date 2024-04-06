#! /usr/bin/env python
from pathlib import Path
from psycopg2.errors import UndefinedFunction
from sqlalchemy import text
from src.core.config import settings
from sqlalchemy import create_engine
from collections import namedtuple
from src.utils import print_info, print_hashtags
import os

engine = create_engine(
    settings.POSTGRES_DATABASE_URI
)

def find_unapplied_dependencies(function_content, function_list):
    dependencies = set()
    for function in function_list:
        if "." + function.name in function_content:
            dependencies.add(function.name)
    return dependencies


def get_name_from_path(path) -> str:
    """
    to get name of function from path (file name)
    """
    basename = os.path.basename(path)
    return os.path.splitext(basename)[0]


def get_function_list_from_files(path_list):
    function = namedtuple("function", ["path", "name", "dependencies"])
    function_list = []
    for path in path_list:
        function_name = get_name_from_path(path)
        function_list.append(function(path, function_name, set()))
    return function_list


def sorted_path_by_dependency(path_list):
    """
    The first function is not depended to any others.
    But the last one maybe depended.
    """
    new_path_list = []
    function_list = get_function_list_from_files(path_list)
    while function_list:
        function = function_list.pop(0)
        function_content = Path(function.path).read_text()
        dependencies = find_unapplied_dependencies(function_content, function_list)
        if dependencies:
            # found dependencies, add to the end of functions again.
            # update dependencies
            function.dependencies.clear()
            function.dependencies.update(dependencies)
            function_list.append(function)
        else:
            new_path_list.append(Path(function.path))

    return new_path_list

def sql_function_entities():
    sql_function_entities = []
    # Find all with the exception the ones in legacy
    function_paths = Path(str(Path().resolve()) + "/src/db/sql/functions").rglob("*.sql")
    function_paths = [p for p in function_paths if "legacy" not in str(p)]
    function_paths = sorted_path_by_dependency(function_paths)
    for p in function_paths:
        sql_function_entities.append(p.read_text())
    return sql_function_entities


def drop_functions():
    # Drop all functions in the schema 'basic'
    stmt_list_functions = text(
        "SELECT proname FROM pg_proc WHERE pronamespace = 'basic'::regnamespace"
    )
    functions = engine.execute(stmt_list_functions).fetchall()
    functions = [f[0] for f in functions]
    for function in functions:
        # Skip trigger functions as they should be dropped by drop_triggers()
        if "trigger" in function:
            continue
        print_info(f"Dropping {function}()")
        statement = f"DROP FUNCTION IF EXISTS basic.{function} CASCADE;"
        try:
            engine.execute(text(statement))
        except UndefinedFunction as e:
            print(e)
    print_info(f"{len(functions)} functions dropped!")


def add_functions():
    sql_function_entities_ = sql_function_entities()
    for function in sql_function_entities_:
        engine.execute(text(function))
        print_info("Adding Function.")
    print_info(f"{len(sql_function_entities_)} functions added!")


def update_functions():
    # It will drop all functions and add them again
    drop_functions()
    print_hashtags()
    add_functions()

#update_functions()