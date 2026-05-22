import re

def to_snake_case(name):
    return re.sub(r"[\s]+", "_", name.strip().casefold())


def rename_column_to_snake_case(df):
    new_column = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_column)