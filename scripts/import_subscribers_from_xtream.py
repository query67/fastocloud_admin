#!/usr/bin/env python3
import argparse
import os
import sys
from datetime import datetime
from mongoengine import connect
import mysql.connector

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.common.subscriber.login.entry import SubscriberUser
from app.common.subscriber.entry import Device
from app.service.service import ServiceSettings

PROJECT_NAME = 'import_subscribers_from_xtream'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog=PROJECT_NAME, usage='%(prog)s [options]')
    parser.add_argument('--mongo_uri', help='MongoDB credentials', default='mongodb://localhost:27017/iptv')
    parser.add_argument('--mysql_host', help='MySQL host', default='localhost')
    parser.add_argument('--mysql_user', help='MySQL username', default='root')
    parser.add_argument('--mysql_password', help='MySQL password', default='')
    parser.add_argument('--mysql_port', help='MySQL port', default=3306)
    parser.add_argument('--server_id', help='Server ID', default='')
    parser.add_argument('--country', help='Subscribers country', default='US')

    argv = parser.parse_args()
    mysql_host = argv.mysql_host
    mysql_user = argv.mysql_user
    mysql_password = argv.mysql_password
    mysql_port = argv.mysql_port
    server_id = argv.server_id
    country = argv.country

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

    sql = 'SELECT username,password,created_at,exp_date FROM users'

    cursor.execute(sql)

    sql_subscribers = cursor.fetchall()

    for sql_entry in sql_subscribers:
        new_user = SubscriberUser.make_subscriber(email=sql_entry['username'], password=sql_entry['password'],
                                                  country=country)
        new_user.status = SubscriberUser.Status.ACTIVE
        created_at = sql_entry['created_at']
        if created_at:
            new_user.created_date = datetime.fromtimestamp(created_at)
        exp_date = sql_entry['exp_date']
        if exp_date:
            new_user.exp_date = datetime.fromtimestamp(exp_date)
        dev = Device(name='Xtream')
        new_user.add_device(dev)
        # save
        new_user.add_server(server)
        server.add_subscriber(new_user)

    cursor.close()
    db.close()
