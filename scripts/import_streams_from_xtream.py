#!/usr/bin/env python3
import argparse
import os
import sys
import json
from mongoengine import connect
import mysql.connector

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.common.stream.entry import ProxyStream
from app.service.service import ServiceSettings
from app.common.utils.utils import is_valid_http_url
import app.common.constants as constants

PROJECT_NAME = 'import_streams_from_xtream'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog=PROJECT_NAME, usage='%(prog)s [options]')
    parser.add_argument('--mongo_uri', help='MongoDB credentials', default='mongodb://localhost:27017/iptv')
    parser.add_argument('--mysql_host', help='MySQL host', default='localhost')
    parser.add_argument('--mysql_user', help='MySQL username', default='root')
    parser.add_argument('--mysql_password', help='MySQL password', default='')
    parser.add_argument('--mysql_port', help='MySQL port', default=3306)
    parser.add_argument('--server_id', help='Server ID', default='')

    argv = parser.parse_args()
    mysql_host = argv.mysql_host
    mysql_user = argv.mysql_user
    mysql_password = argv.mysql_password
    mysql_port = argv.mysql_port
    server_id = argv.server_id

    mongo = connect(host=argv.mongo_uri)
    if not mongo:
        sys.exit(1)

    server = ServiceSettings.objects(id=server_id).first()
    if not server:
        sys.exit(1)

    db = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        passwd=mysql_password,
        database='xtream_iptvpro'
    )

    cursor = db.cursor(dictionary=True)

    sql = 'SELECT stream_source, stream_display_name, stream_icon, channel_id from streams'

    cursor.execute(sql)

    sql_streams = cursor.fetchall()

    for sql_entry in sql_streams:
        stream = ProxyStream.make_stream(server)
        urls = json.loads(sql_entry['stream_source'])
        if not len(urls):
            continue

        stream.output.urls[0].uri = urls[0]
        stream.name = sql_entry['stream_display_name']
        tvg_logo = sql_entry['stream_icon']
        if len(tvg_logo) < constants.MAX_URL_LENGTH:
            if is_valid_http_url(tvg_logo, timeout=0.1):
                stream.tvg_logo = tvg_logo
        epg_id = sql_entry['channel_id']
        if epg_id:
            stream.tvg_id = epg_id

        stream.save()
        server.streams.append(stream)

    server.save()

    cursor.close()
    db.close()
