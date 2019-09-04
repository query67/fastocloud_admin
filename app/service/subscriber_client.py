from app.common.subscriber.entry import Subscriber, Device
from pyfastocloud.subscriber_client import SubscriberClient
from pyfastocloud.client import make_utc_timestamp


class SubscriberConnection(SubscriberClient):
    def __init__(self, sock, addr, handler):
        super(SubscriberConnection, self).__init__(sock, addr, handler)
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
        data = self.read_command()
        if not data:
            return False

        self.process_commands(data)
        return True

    def gen_request_id(self) -> int:
        current_value = self._request_id
        self._request_id += 1
        return current_value
