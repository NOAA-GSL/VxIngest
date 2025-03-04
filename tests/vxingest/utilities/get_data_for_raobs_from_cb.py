import os
import sys
from pathlib import Path

import yaml
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from tabulate import tabulate


def connect_cb():
    """
    create a couchbase connection and maintain the collection and cluster objects.
    """
    credentials_file = os.environ["CREDENTIALS"]
    assert Path(credentials_file).is_file() is True, (
        f"*** credentials_file file {credentials_file} can not be found!"
    )
    with Path(credentials_file).open(encoding="utf-8") as _f:
        _yaml_data = yaml.load(_f, yaml.SafeLoader)
    cb_connection = {}
    cb_connection["host"] = _yaml_data["cb_host"]
    cb_connection["user"] = _yaml_data["cb_user"]
    cb_connection["password"] = _yaml_data["cb_password"]
    cb_connection["bucket"] = _yaml_data["cb_bucket"]
    cb_connection["collection"] = _yaml_data["cb_collection"]
    cb_connection["scope"] = _yaml_data["cb_scope"]
    # I really want the RAOB collection
    cb_connection["collection"] = "RAOB"

    options = ClusterOptions(
        PasswordAuthenticator(cb_connection["user"], cb_connection["password"])
    )
    cb_connection["cluster"] = Cluster(cb_connection["host"], options)
    cb_connection["collection"] = (
        cb_connection["cluster"]
        .bucket(cb_connection["bucket"])
        .collection(cb_connection["collection"])
    )
    return cb_connection


def main():
    wmoid = sys.argv[1]
    _statement = f"""SELECT
    d.data.['{wmoid}'].['pressure'] press,
            d.data.['{wmoid}'].['height'] z,
            ROUND(d.data.['{wmoid}'].['temperature'],4) t,
            ROUND(d.data.['{wmoid}'].['dewpoint'],4) dp,
            ROUND(d.data.['{wmoid}'].['relative_humidity'],4) rh,
            d.data.['{wmoid}'].['wind_direction'] wd,
            d.data.['{wmoid}'].['wind_speed'] ws
            FROM vxdata._default.RAOB AS d
            WHERE type='DD'
            AND subset='RAOB'
            AND docType='obs'
            AND subDocType = 'prepbufr'
            AND fcstValidISO = '2024-07-31T00:00:00Z'
            AND d.data.['{wmoid}'].['pressure'] IN [1000,850,700,500,400,300,250,200,150,100,70,50,30,20]
            ORDER BY d.data.['{wmoid}'].['pressure'] DESC;"""
    data_iter = connect_cb()["cluster"].query(_statement)
    table = [
        [
            "press",
            "z",
            "t",
            "dp",
            "rh",
            "wd",
            "ws",
        ]
    ]

    for row in data_iter.rows():
        table.append(
            [
                row["press"] if row["press"] != "null" else "null",
                row["z"] if row["z"] else "null",
                row["t"] if row["t"] else "null",
                row["dp"] if row["dp"] else "null",
                row["rh"] if row["rh"] else "null",
                row["wd"] if row["wd"] else "null",
                row["ws"] if row["ws"] else "null",
            ]
        )
    print(tabulate(table, headers="firstrow", tablefmt="plain"))


if __name__ == "__main__":
    main()
