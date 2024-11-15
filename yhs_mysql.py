from pymysql import cursors, connect
import pandas as pd
from yhs_common import *
from typing import Any, Text


def __get_yhsdb_query(conn: connect, sql: str, args: tuple = None) -> pd.DataFrame:
    # log(sql)
    cursor = conn.cursor(cursors.DictCursor)
    try:
        cursor.execute(query=sql, args=args)
        result: tuple[dict[Text, Any], ...] = cursor.fetchall()
    except Exception as e:
        log(e)
        log(f"get sql error = {sql} : {args}")
        result = ()
    finally:
        cursor.close()
    return pd.DataFrame(result)


def __get_one_yhsdb(conn: connect, sql: str, args: tuple = None) -> Any:
    # log(sql)
    df: pd.DataFrame = __get_yhsdb_query(conn=conn, sql=sql, args=args)
    if len(df) == 0:
        return None
    return df.iloc[0][0]
