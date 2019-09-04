from app.common.service.entry import ServiceSettings
from app.service.service import Service
from app.service.subscriber_client import SubscriberConnection
from app.common.subscriber.entry import Subscriber
from pyfastocloud.subscriber_client import Commands
from pyfastocloud.client import make_utc_timestamp
from pyfastocloud.client_handler import IClientHandler, Request, Response, ClientStatus

from gevent import socket
from gevent import select


def check_is_auth_client(client) -> bool:
    if not client:
        return False

    return client.is_active()


class ServiceManager(IClientHandler):
    SUBSCRIBER_PORT = 6000
    BANDWIDTH_PORT = 5000
    PING_SUBSCRIBERS_SEC = 60

    def __init__(self, host: str, port: int, socketio):
        self._host = host
        self._port = port
        self._socketio = socketio
        self._stop_listen = False
        self._servers_pool = []
        serversock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serversock.bind((host, ServiceManager.SUBSCRIBER_PORT))
        serversock.listen(10)
        self._subscribers_server_socket = serversock
        self._subscribers = []

    def stop(self):
        self._stop_listen = True

    def find_or_create_server(self, settings: ServiceSettings) -> Service:
        for server in self._servers_pool:
            if server.id == settings.id:
                return server

        server = Service(self._host, self._port, self._socketio, settings)
        self.__add_server(server)
        return server

    def refresh(self):
        while not self._stop_listen:
            rsockets = []
            rsockets.append(self._subscribers_server_socket)
            for client in self._subscribers:
                rsockets.append(client.socket())
            for server in self._servers_pool:
                if server.is_connected():
                    rsockets.append(server.socket())

            readable, writeable, _ = select.select(rsockets, [], [], 1)
            ts_sec = make_utc_timestamp() / 1000
            for read in readable:
                # income subscriber connection
                if self._subscribers_server_socket == read:
                    csock, addr = read.accept()
                    subs = SubscriberConnection(csock, addr, self)
                    self.__add_subscriber(subs)
                    continue

                # subscriber read
                for client in self._subscribers:
                    if client.socket() == read:
                        res = client.recv_data()
                        if not res:
                            self.__remove_subscriber(client)
                            client.disconnect()
                        break

                for server in self._servers_pool:
                    if server.socket() == read:
                        server.recv_data()
                        break

            for client in self._subscribers:
                if ts_sec - client.last_ping_ts > ServiceManager.PING_SUBSCRIBERS_SEC:
                    client.ping()
                    client.last_ping_ts = ts_sec

    def process_response(self, client, req: Request, resp: Response):
        if req.method == Commands.SERVER_PING_COMMAND:
            self._handle_server_ping_command(client, resp)
        elif req.method == Commands.SERVER_GET_CLIENT_INFO_COMMAND:
            self._handle_server_get_client_info(client, resp)

    def process_request(self, client, req: Request):
        if not req:
            return

        if req.method == Commands.ACTIVATE_COMMAND:
            self._handle_activate_subscriber(client, req.id, req.params)
        elif req.method == Commands.GET_SERVER_INFO_COMMAND:
            self._handle_get_server_info(client, req.id, req.params)
        elif req.method == Commands.CLIENT_PING_COMMAND:
            self._handle_client_ping(client, req.id, req.params)
        elif req.method == Commands.GET_CHANNELS:
            self._handle_get_channels(client, req.id, req.params)
        elif req.method == Commands.GET_RUNTIME_CHANNEL_INFO:
            self._handle_get_runtime_channel_info(client, req.id, req.params)
        else:
            pass

    def on_client_state_changed(self, client, status: ClientStatus):
        pass

        # protected

    def _handle_server_ping_command(self, client, resp: Response):
        pass

    def _handle_server_get_client_info(self, client, resp: Response):
        pass

    def _handle_activate_subscriber(self, client, cid: str, params: dict):
        login = params[Subscriber.EMAIL_FIELD]
        password_hash = params[Subscriber.PASSWORD_FIELD]
        device_id = params['device_id']

        check_user = Subscriber.objects(email=login, class_check=False).first()
        if not check_user:
            client.activate_fail(cid, 'User not found')
            return

        if check_user.status == Subscriber.Status.NOT_ACTIVE:
            client.activate_fail(cid, 'User not active')
            return

        if check_user.status == Subscriber.Status.BANNED:
            client.activate_fail(cid, 'Banned user')
            return

        if check_user[Subscriber.PASSWORD_FIELD] != password_hash:
            client.activate_fail(cid, 'User invalid password')
            return

        found_device = check_user.find_device(device_id)
        if not found_device:
            client.activate_fail(cid, 'Device not found')
            return

        user_connections = self.get_user_connections_by_email(login)
        for conn in user_connections:
            if conn.device == found_device:
                client.activate_fail(cid, 'Device in use')
                return

        client.activate_success(cid)
        client.info = check_user
        client.device = found_device

    def _handle_get_server_info(self, client, cid: str, params: dict):
        if not check_is_auth_client(client):
            client.check_activate_fail(cid, 'User not active')
            client.disconnect()
            return

        client.get_server_info_success(cid, '{0}:{1}'.format(self._host, ServiceManager.BANDWIDTH_PORT))

    def _handle_client_ping(self, client, cid: str, params: dict):
        pass

    def _handle_get_channels(self, client, cid: str, params: dict):
        if not check_is_auth_client(client):
            client.check_activate_fail(cid, 'User not active')
            client.disconnect()
            return

        channels = client.info.get_streams()
        client.get_channels_success(cid, channels)

    def _handle_get_runtime_channel_info(self, client, cid: str, params: dict):
        if not check_is_auth_client(client):
            client.check_activate_fail(cid, 'User not active')
            client.disconnect()
            return

        sid = params['id']
        watchers = self.get_watchers_by_stream_id(sid)
        client.current_stream_id = sid
        client.get_runtime_channel_info_success(cid, sid, watchers)

    # private
    def __add_server(self, server: Service):
        self._servers_pool.append(server)

    def __add_subscriber(self, subs: SubscriberConnection):
        self._subscribers.append(subs)

    def __remove_subscriber(self, subs: SubscriberConnection):
        self._subscribers.remove(subs)

    def get_watchers_by_stream_id(self, sid: str):
        total = 0
        for user in self._subscribers:
            if user.current_stream_id == sid:
                total += 1

        return total

    def get_user_connections_by_email(self, email) -> list:
        connections = []
        for user in self._subscribers:
            if user.info and user.info.email == email:
                connections.append(user)

        return connections

    def send_message(self, email: str, message: str, ttl: int):
        for user in self._subscribers:
            if user.info and user.info.email == email:
                user.send_message(message, ttl * 1000)
