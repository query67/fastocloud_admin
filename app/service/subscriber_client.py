from app.common.subscriber.entry import Subscriber, Device
from pyfastocloud.subscriber_client import SubscriberClient
from pyfastocloud.client import make_utc_timestamp


class SubscriberConnection(object):
    def __init__(self, sock, addr, handler):
        self._client = SubscriberClient(sock, addr, handler)
        self._info = None
        self._current_stream_id = str()
        self._device = None
        self._last_ping_ts = make_utc_timestamp() / 1000
        self._request_id = 0

    @property
    def info(self) -> Subscriber:
        return self._info

    @info.setter
    def info(self, value):
        self._info = value

    @property
    def current_stream_id(self) -> str:
        return self._current_stream_id

    @current_stream_id.setter
    def current_stream_id(self, value):
        self._current_stream_id = value

    @property
    def device(self) -> Device:
        return self._device

    @device.setter
    def device(self, value):
        self._device = value

    @property
    def last_ping_ts(self) -> float:
        return self._last_ping_ts

    @last_ping_ts.setter
    def last_ping_ts(self, value):
        self._last_ping_ts = value

    def recv_data(self) -> bool:
        data = self._client.read_command()
        if not data:
            return False

        self._client.process_commands(data)
        return True

    def socket(self):
        return self._client.socket()

    def ping(self):
        return self._client.ping(self._gen_request_id())

    def disconnect(self):
        return self._client.disconnect()

    def is_active(self):
        return self._client.is_active()

    def activate_fail(self, cid: str, error: str):
        return self._client.activate_fail(cid, error)

    def activate_success(self, cid: str):
        return self._client.activate_success(cid)

    def check_activate_fail(self, command_id: str, error: str):
        return self._client.check_activate_fail(command_id, error)

    def get_runtime_channel_info_success(self, command_id, sid: str, watchers: int):
        return self._client.get_runtime_channel_info_success(command_id, sid, watchers)

    def get_channels_success(self, cid: str):
        channels = self.info.get_streams()
        return self._client.get_channels_success(cid, channels)

    def get_server_info_success(self, cid: str, bandwidth_host: str):
        return self._client.get_server_info_success(cid, bandwidth_host)

    # private
    def _gen_request_id(self) -> int:
        current_value = self._request_id
        self._request_id += 1
        return current_value
