#!/usr/bin/env python

import json
import os

import click

from pgload.load import do_replace


@click.command()
@click.option("--source", required=True)
def do_replace_cli(source):
    vcap_services_str = os.getenv("VCAP_SERVICES")
    if not vcap_services_str:
        raise Exception("VCAP_SERVICES environment variable missing from environment!")
    vcap_services = json.loads(vcap_services_str)
    credentials = vcap_services["postgres"][0]["Credentials"]
    host = credentials["host"]
    database = credentials["name"]
    user = credentials["username"]
    password = credentials["password"]
    port = credentials["port"]

    return do_replace(source, host, database, user, password, port)


if __name__ == "__main__":
    do_replace_cli()
