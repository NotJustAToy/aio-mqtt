# Copyright 2019 Not Just A Toy Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
import struct
import enum
import uuid
import time
import collections
import typing as ty
import asyncio as aio
from ssl import SSLContext

import aiochannel

from .version import __version__

__all__ = (
    '__version__',
    'QOSLevel',
    'ConnectReturnCode',
    'SubscribeReturnCode',
    'MQTTClientError',
    'MQTTClientProtocolError',
    'MQTTClientChannelClosedError',
    'MQTTClientPingResponseWaitingTimedOutError',
    'MQTTClientConnectionError',
    'MQTTClientNotConnectedError',
    'MQTTClientAlreadyConnectedError',
    'MQTTClientConnectionClosedError',
    'MQTTClientConnectionCanceledError',
    'MQTTClientConnectionFailedError',
    'MQTTClientConnectionRefusedError',
    'MQTTClientConnectionLostError',
    'PublishableMessage',
    'ReceivedMessage',
    'MQTTClient',
)

try:
    # Use monotonic clock if available
    time_func = time.monotonic
except AttributeError:
    time_func = time.time


class ProtocolVersion(enum.IntEnum):
    MQTTv311 = 4


class PacketType(enum.IntEnum):
    CONNECT = 0x10
    CONNACK = 0x20
    PUBLISH = 0x30
    PUBACK = 0x40
    PUBREC = 0x50
    PUBREL = 0x60
    PUBCOMP = 0x70
    SUBSCRIBE = 0x80
    SUBACK = 0x90
    UNSUBSCRIBE = 0xA0
    UNSUBACK = 0xB0
    PINGREQ = 0xC0
    PINGRESP = 0xD0
    DISCONNECT = 0xE0


class QOSLevel(enum.IntEnum):
    QOS_0 = 0x00
    QOS_1 = 0x01
    QOS_2 = 0x02


class ConnectReturnCode(enum.IntEnum):
    CONNECTION_ACCEPTED = 0x00
    UNACCEPTABLE_PROTOCOL_VERSION = 0x01
    IDENTIFIER_REJECTED = 0x02
    SERVER_UNAVAILABLE = 0x03
    BAD_USERNAME_PASSWORD = 0x04
    NOT_AUTHORIZED = 0x05


CONNECT_RETURN_CODE_DESCRIPTIONS = {
    ConnectReturnCode.CONNECTION_ACCEPTED:
        "Connection accepted",
    ConnectReturnCode.UNACCEPTABLE_PROTOCOL_VERSION:
        "The server does not support the level of the MQTT protocol requested by the client",
    ConnectReturnCode.IDENTIFIER_REJECTED:
        "The client identifier is correct UTF-8 but not allowed by the server",
    ConnectReturnCode.SERVER_UNAVAILABLE:
        "The network connection has been made but the MQTT service is unavailable",
    ConnectReturnCode.BAD_USERNAME_PASSWORD:
        "The data in the user name or password is malformed",
    ConnectReturnCode.NOT_AUTHORIZED:
        "The client is not authorized to connect"
}


class SubscribeReturnCode(enum.IntEnum):
    SUCCESS_MAXIMUM_QOS_0 = 0x00
    SUCCESS_MAXIMUM_QOS_1 = 0x01
    SUCCESS_MAXIMUM_QOS_2 = 0x02
    FAILURE = 0x80


MAXIMUM_PAYLOAD_SIZE: int = 268435455
MAXIMUM_TOPIC_LENGTH: int = 65535

TOPIC_FILTER_REGEX = re.compile(r'^/?(\+|[\w-]+)(/(\+|[\w-]+))*(/#)?$')
TOPIC_NAME_REGEX = re.compile(r'^/?[\w-]+(/[\w-]+)*$')


class MQTTClientError(Exception):
    pass


class MQTTClientProtocolError(MQTTClientError):
    pass


class MQTTClientChannelClosedError(MQTTClientError):
    pass


class MQTTClientPingResponseWaitingTimedOutError(MQTTClientError):
    pass


class MQTTClientConnectionError(MQTTClientError):
    pass


class MQTTClientNotConnectedError(MQTTClientConnectionError):
    pass


class MQTTClientAlreadyConnectedError(MQTTClientConnectionError):
    pass


class MQTTClientConnectionClosedError(MQTTClientConnectionError):
    pass


class MQTTClientConnectionCanceledError(MQTTClientConnectionError):
    pass


class MQTTClientConnectionFailedError(MQTTClientConnectionError):
    pass


class MQTTClientConnectionRefusedError(MQTTClientConnectionError):

    def __init__(self, message: str, return_code: ConnectReturnCode) -> None:
        self.message = message
        self.return_code = return_code

    def __str__(self) -> str:
        return self.message


class MQTTClientConnectionLostError(MQTTClientConnectionError):
    pass


class ForbiddenReadingError(Exception):
    pass


class LimitedStreamReader(object):

    __slots__ = ('reader', 'remaining_length')

    def __init__(self, reader: aio.StreamReader, remaining_length: int) -> None:
        self.reader = reader
        self.remaining_length = remaining_length

    async def read(self, size: ty.Optional[int] = None) -> bytes:
        if size is None:
            size = self.remaining_length

        elif size > self.remaining_length:
            raise ForbiddenReadingError(
                f"You are trying to read {size - self.remaining_length} bytes over the limit")

        self.remaining_length -= size

        return await self.reader.readexactly(size)

    def __bool__(self) -> bool:
        return self.remaining_length > 0


class PublishableMessage(object):

    __slots__ = ('topic_name', 'payload', 'qos', 'retain')

    payload: ty.Union[bytes, bytearray]

    def __init__(
            self,
            topic_name: str,
            payload: ty.Optional[ty.Union[int, float, str, bytes, bytearray]] = None,
            qos: QOSLevel = QOSLevel.QOS_0,
            retain: bool = False
    ) -> None:
        if not TOPIC_NAME_REGEX.match(topic_name) or len(topic_name) > MAXIMUM_TOPIC_LENGTH:
            raise ValueError("Invalid topic name")

        self.topic_name = topic_name

        if payload is None:
            self.payload = b''

        elif isinstance(payload, (int, float)):
            self.payload = str(payload).encode('ascii')

        elif isinstance(payload, str):
            self.payload = payload.encode('utf-8', errors='strict')

        else:
            self.payload = payload

        if len(self.payload) > MAXIMUM_PAYLOAD_SIZE:
            raise ValueError("Payload too large")

        self.qos = qos

        self.retain = retain


class ReceivedMessage(ty.NamedTuple):
    topic_name: str
    payload: bytes
    dup: bool
    qos: QOSLevel
    retain: bool


class InflightMessageState(enum.Enum):
    WAIT_FOR_PUBACK = enum.auto()
    WAIT_FOR_PUBREC = enum.auto()
    WAIT_FOR_PUBREL = enum.auto()
    WAIT_FOR_PUBCOMP = enum.auto()


T = ty.TypeVar('T', PublishableMessage, ReceivedMessage)


class InflightMessage(ty.Generic[T]):

    __slots__ = ('message', 'state', 'timestamp')

    message: T

    def __init__(
            self,
            message: T,
            state: InflightMessageState
    ) -> None:
        self.message = message
        self.state = state
        self.timestamp = time_func()

    def update_state(self, new_state: InflightMessageState) -> None:
        self.state = new_state
        self.update_timestamp()

    def update_timestamp(self, new_timestamp: ty.Optional[int] = None) -> None:
        self.timestamp = new_timestamp or time_func()


def packet_id_generator() -> ty.Generator[int, None, None]:
    packet_id = 1
    while True:
        yield packet_id
        packet_id = packet_id + 1 if packet_id < 65536 else 1


class MQTTClient(object):

    def __init__(
            self,
            message_retry_interval: int = 20,
            ping_response_waiting_timeout: int = 20,
            loop: ty.Optional[aio.AbstractEventLoop] = None
    ) -> None:
        if message_retry_interval < 1:
            raise ValueError("Message retry interval must be >= 1")
        self._message_retry_interval = message_retry_interval
        if ping_response_waiting_timeout < 1:
            raise ValueError("Ping response waiting timeout must be >= 1")
        self._ping_response_waiting_timeout = ping_response_waiting_timeout
        self._loop = loop or aio.get_event_loop()
        self._connected = aio.Event(loop=self._loop)
        self._waiters: ty.DefaultDict[ty.Hashable, ty.Deque[aio.Future]] = collections.defaultdict(
            collections.deque)
        self._tasks: ty.List[aio.Task] = []
        self._received_messages: ty.Optional[aiochannel.Channel] = None
        self._packet_ids = packet_id_generator()
        self._incoming_messages: ty.Dict[int, InflightMessage[ReceivedMessage]] = {}
        self._outgoing_messages: ty.Dict[int, InflightMessage[PublishableMessage]] = {}
        self._reader: ty.Optional[aio.StreamReader] = None
        self._writer: ty.Optional[aio.StreamWriter] = None
        self._drain_lock = aio.Lock(loop=self._loop)
        self._ping_response_received: bool = False

    async def connect(
            self,
            host: str,
            port: int = 1883,
            ssl: ty.Union[bool, SSLContext] = False,
            keepalive: int = 60,
            client_id: ty.Optional[str] = None,
            clean_session: bool = True,
            will_message: ty.Optional[PublishableMessage] = None,
            username: ty.Optional[str] = None,
            password: ty.Optional[str] = None
    ) -> bool:
        if len(host) == 0:
            raise ValueError("Invalid host")

        if port <= 0:
            raise ValueError("Invalid port number")

        if keepalive < 0:
            raise ValueError("Keepalive must be >= 0")

        if not client_id and not clean_session:
            raise ValueError("A client id must be provided if clean session is False")

        if self._connected.is_set():
            raise MQTTClientAlreadyConnectedError()

        client_id = client_id or uuid.uuid4().hex

        await self._open_connection(host=host, port=port, ssl=ssl)

        try:
            self._run_tasks(keepalive=keepalive)
            self._open_channel()
            if clean_session:
                self._clean_session()
        except Exception as e:
            await self._disconnect(e)
            raise

        try:
            await self._send_connect(
                keepalive=keepalive,
                client_id=client_id,
                clean_session=clean_session,
                will_message=will_message,
                username=username,
                password=password
            )
        except aio.CancelledError:
            await self._disconnect(MQTTClientConnectionCanceledError())
            raise

        except Exception as e:
            await self._disconnect(e)
            raise

        try:
            return await self._wait(PacketType.CONNACK)
        except aio.CancelledError:
            await self._disconnect(MQTTClientConnectionCanceledError())
            raise

        except Exception as e:
            await self._disconnect(e)
            raise

    async def disconnect(self) -> None:
        if not self._connected.is_set():
            raise MQTTClientNotConnectedError()

        try:
            await self._send_disconnect()
        except aio.CancelledError:
            raise

        except Exception as e:
            await self._disconnect(e)
            raise

        await self._disconnect()

    async def subscribe(self, *topics: ty.Tuple[str, QOSLevel]) -> ty.Tuple[SubscribeReturnCode]:
        if not self._connected.is_set():
            raise MQTTClientNotConnectedError()

        if any(not TOPIC_FILTER_REGEX.match(topic_filter) or len(topic_filter) > MAXIMUM_TOPIC_LENGTH
               for topic_filter, _ in topics):
            raise ValueError("Invalid topic filter")

        packet_id = next(self._packet_ids)

        try:
            await self._send_subscribe(topics, packet_id)
        except aio.CancelledError:
            raise

        except Exception as e:
            await self._disconnect(e)
            raise

        return await self._wait((PacketType.SUBACK, packet_id))

    async def unsubscribe(self, *topic_filters: str) -> None:
        if not self._connected.is_set():
            raise MQTTClientNotConnectedError()

        if any(not topic_filter or len(topic_filter) > MAXIMUM_TOPIC_LENGTH for topic_filter in topic_filters):
            raise ValueError("Invalid topic filter")

        packet_id = next(self._packet_ids)

        try:
            await self._send_unsubscribe(topic_filters, packet_id)
        except aio.CancelledError:
            raise

        except Exception as e:
            await self._disconnect(e)
            raise

        await self._wait((PacketType.UNSUBACK, packet_id))

    async def publish(self, message: PublishableMessage) -> None:
        if not self._connected.is_set():
            raise MQTTClientNotConnectedError()

        packet_id = next(self._packet_ids)

        try:
            await self._send_publish(message, packet_id)
        except aio.CancelledError:
            raise

        except Exception as e:
            await self._disconnect(e)
            raise

        try:
            if message.qos == QOSLevel.QOS_1:
                self._outgoing_messages[packet_id] = InflightMessage(
                    message, InflightMessageState.WAIT_FOR_PUBACK)

                await self._wait((PacketType.PUBACK, packet_id))

            elif message.qos == QOSLevel.QOS_2:
                self._outgoing_messages[packet_id] = InflightMessage(
                    message, InflightMessageState.WAIT_FOR_PUBREC)

                await self._wait((PacketType.PUBCOMP, packet_id))
        except aio.CancelledError:
            self._outgoing_messages.pop(packet_id, None)
            raise

    async def receive_message(self) -> ReceivedMessage:
        await self._connected.wait()

        assert self._received_messages is not None

        try:
            return await self._received_messages.get()
        except aiochannel.ChannelClosed as e:
            raise MQTTClientChannelClosedError() from e

    async def wait_connecting(self) -> None:
        await self._connected.wait()

    async def wait_disconnecting(self) -> None:
        return await self._wait('disconnect', cancel_previous=False)

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    async def _wait(self, key: ty.Hashable, cancel_previous: bool = True) -> ty.Any:
        waiters = self._waiters[key]

        if cancel_previous:
            for waiter in waiters:
                if not waiter.done():
                    waiter.cancel()

        waiter = aio.Future(loop=self._loop)
        waiters.append(waiter)
        try:
            return await waiter
        finally:
            try:
                waiters.remove(waiter)
            except ValueError:
                pass

            if not waiters:
                self._waiters.pop(key, None)

    def _wakeup(
            self,
            key: ty.Hashable,
            result: ty.Optional[ty.Any] = None,
            exception: ty.Optional[BaseException] = None
    ) -> None:
        waiters = self._waiters.get(key)

        if waiters is None:
            return

        for waiter in waiters:
            if waiter.done():
                continue

            if result is not None:
                waiter.set_result(result)

            elif exception is not None:
                waiter.set_exception(exception)

            else:
                waiter.set_result(None)

    def _wakeup_all(
            self,
            result: ty.Optional[ty.Any] = None,
            exception: ty.Optional[BaseException] = None
    ) -> None:
        for key in self._waiters.keys():
            self._wakeup(key, result=result, exception=exception)

    async def _open_connection(self, *, host: str, port: int, ssl: ty.Union[bool, SSLContext]) -> None:
        try:
            self._reader, self._writer = await aio.open_connection(
                host=host, port=port, loop=self._loop, ssl=ssl)
        except (ConnectionError, OSError) as e:
            raise MQTTClientConnectionFailedError() from e

    def _close_connection(self) -> None:
        if self._writer is not None:
            try:
                self._writer.close()
            except:  # noqa: E722
                pass
            self._writer = None

        if self._reader is not None:
            self._reader = None

    def _run_tasks(self, *, keepalive: int) -> None:
        self._tasks.append(self._loop.create_task(self._reading_loop()))
        if keepalive > 0:
            self._tasks.append(self._loop.create_task(self._keepalive_mechanism(keepalive)))
        self._tasks.append(self._loop.create_task(self._message_retry_check()))

    async def _cancel_tasks(self) -> None:
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except aio.CancelledError:
                pass
        self._tasks.clear()

    def _open_channel(self) -> None:
        self._received_messages = aiochannel.Channel(loop=self._loop)

    def _close_channel(self) -> None:
        if self._received_messages is not None:
            if not self._received_messages.closed():
                self._received_messages.close()
            self._received_messages = None

    def _clean_session(self) -> None:
        self._incoming_messages.clear()
        self._outgoing_messages.clear()

    async def _send(self, packet: ty.Union[bytes, bytearray]) -> None:
        assert self._writer is not None
        try:
            self._writer.write(packet)
            async with self._drain_lock:
                await self._writer.drain()
        except ConnectionResetError as e:
            raise MQTTClientConnectionLostError() from e

    async def _disconnect(self, exception: ty.Optional[BaseException] = None) -> None:
        self._connected.clear()
        self._close_channel()
        await self._cancel_tasks()
        self._close_connection()
        self._wakeup('disconnect', exception=exception)
        self._wakeup_all(exception=MQTTClientConnectionClosedError())

    @staticmethod
    def _pack_remaining_length(packet: bytearray, remaining_length: int) -> None:
        remaining_bytes = []
        while True:
            byte = remaining_length % 128
            remaining_length = remaining_length // 128
            # If there are more digits to encode, set the top bit of this digit
            if remaining_length > 0:
                byte |= 0x80

            remaining_bytes.append(byte)
            packet.append(byte)
            if remaining_length == 0:
                break

    @staticmethod
    def _pack_string(packet: bytearray, data: ty.Union[str, bytes]) -> None:
        if isinstance(data, str):
            data = data.encode('utf-8')
        packet.extend(struct.pack('!H', len(data)))
        packet.extend(data)

    async def _send_connect(self,
                            *,
                            keepalive: int,
                            client_id: str,
                            clean_session: bool,
                            will_message: ty.Optional[PublishableMessage] = None,
                            username: ty.Optional[str] = None,
                            password: ty.Optional[str] = None) -> None:
        remaining_length = 2 + 4 + 1 + 1 + 2 + 2 + len(client_id)

        connect_flags = 0
        if clean_session:
            connect_flags |= 0x02

        if will_message is not None:
            remaining_length += 2 + len(will_message.topic_name) + 2 + len(will_message.payload)
            connect_flags |= 0x04 | ((will_message.qos & 0x03) << 3) | ((will_message.retain & 0x01) << 5)

        if username is not None:
            remaining_length += 2 + len(username)
            connect_flags |= 0x80
            if password is not None:
                connect_flags |= 0x40
                remaining_length += 2 + len(password)

        packet = bytearray()
        packet.append(PacketType.CONNECT)

        self._pack_remaining_length(packet, remaining_length)
        packet.extend(struct.pack('!H4sBBH', 4, b'MQTT', ProtocolVersion.MQTTv311, connect_flags, keepalive))

        self._pack_string(packet, client_id)

        if will_message is not None:
            self._pack_string(packet, will_message.topic_name)
            self._pack_string(packet, will_message.payload)

        if username is not None:
            self._pack_string(packet, username)

            if password is not None:
                self._pack_string(packet, password)

        await self._send(packet)

    async def _send_packet_with_packet_id(self, packet_type: PacketType, packet_id: int, flags: int = 0x0) -> None:
        remaining_length = 2
        packet = struct.pack('!BBH', packet_type | flags, remaining_length, packet_id)
        await self._send(packet)

    async def _send_puback(self, packet_id: int) -> None:
        await self._send_packet_with_packet_id(PacketType.PUBACK, packet_id)

    async def _send_pubrec(self, packet_id: int) -> None:
        await self._send_packet_with_packet_id(PacketType.PUBREC, packet_id)

    async def _send_pubrel(self, packet_id: int) -> None:
        await self._send_packet_with_packet_id(PacketType.PUBREL, packet_id, flags=0x2)

    async def _send_pubcomp(self, packet_id: int) -> None:
        await self._send_packet_with_packet_id(PacketType.PUBCOMP, packet_id)

    async def _send_simple_packet(self, packet_type: PacketType) -> None:
        remaining_length = 0
        packet = struct.pack('!BB', packet_type, remaining_length)
        await self._send(packet)

    async def _send_disconnect(self) -> None:
        await self._send_simple_packet(PacketType.DISCONNECT)

    async def _send_pingreq(self) -> None:
        await self._send_simple_packet(PacketType.PINGREQ)

    async def _send_subscribe(self, topics: ty.Iterable[ty.Tuple[str, QOSLevel]], packet_id: int) -> None:
        remaining_length = 2
        for topic_filter, _ in topics:
            remaining_length += 2 + len(topic_filter) + 1

        command = PacketType.SUBSCRIBE | 0x2
        packet = bytearray()
        packet.append(command)
        self._pack_remaining_length(packet, remaining_length)
        packet.extend(struct.pack('!H', packet_id))
        for topic_filter, qos in topics:
            self._pack_string(packet, topic_filter)
            packet.append(qos)

        await self._send(packet)

    async def _send_unsubscribe(self, topic_filters: ty.Iterable[str], packet_id: int) -> None:
        remaining_length = 2
        for topic_filter in topic_filters:
            remaining_length += 2 + len(topic_filter)

        command = PacketType.UNSUBSCRIBE | 0x2
        packet = bytearray()
        packet.append(command)
        self._pack_remaining_length(packet, remaining_length)
        packet.extend(struct.pack('!H', packet_id))
        for topic_filter in topic_filters:
            self._pack_string(packet, topic_filter)

        await self._send(packet)

    async def _send_publish(self, message: PublishableMessage, packet_id: int, dup: bool = False) -> None:
        header = PacketType.PUBLISH | ((dup & 0x1) << 3) | (message.qos << 1) | message.retain
        packet = bytearray()
        packet.append(header)

        remaining_length = 2 + len(message.topic_name) + len(message.payload)

        if message.qos > QOSLevel.QOS_0:
            remaining_length += 2

        self._pack_remaining_length(packet, remaining_length)
        self._pack_string(packet, message.topic_name)

        if message.qos > QOSLevel.QOS_0:
            packet.extend(struct.pack('!H', packet_id))

        packet.extend(message.payload)

        await self._send(packet)

    async def _handle_connack(self, reader: LimitedStreamReader) -> None:
        flags, return_code = struct.unpack('!BB', await reader.read(2))
        session_present = bool(flags & 0x01)

        if return_code == ConnectReturnCode.CONNECTION_ACCEPTED:
            self._connected.set()
            self._wakeup(PacketType.CONNACK, result=session_present)

        else:
            self._wakeup(PacketType.CONNACK, exception=MQTTClientConnectionRefusedError(
                CONNECT_RETURN_CODE_DESCRIPTIONS[return_code], return_code))

    async def _handle_suback(self, reader: LimitedStreamReader) -> None:
        packet_id, = struct.unpack('!H', await reader.read(2))
        return_codes = []
        while True:
            return_code, = struct.unpack('!B', await reader.read(1))
            return_codes.append(SubscribeReturnCode(return_code))
            if not reader:
                break

        self._wakeup((PacketType.SUBACK, packet_id), result=return_codes)

    async def _handle_unsuback(self, reader: LimitedStreamReader) -> None:
        packet_id, = struct.unpack('!H', await reader.read(2))

        self._wakeup((PacketType.UNSUBACK, packet_id))

    async def _handle_publish(self, flags, reader: LimitedStreamReader) -> None:
        dup = (flags & 0x08) >> 3
        qos = (flags & 0x06) >> 1
        retain = (flags & 0x01)

        topic_name_length, = struct.unpack('!H', await reader.read(2))
        if topic_name_length:
            data = await reader.read(topic_name_length)
            try:
                topic_name = data.decode('utf-8')
            except UnicodeDecodeError:
                topic_name = str(data)

        else:
            topic_name = ''

        if qos > QOSLevel.QOS_0:
            packet_id, = struct.unpack('!H', await reader.read(2))

        else:
            packet_id = None

        payload = await reader.read()

        message = ReceivedMessage(topic_name, payload, bool(dup), QOSLevel(qos), bool(retain))

        assert self._received_messages is not None

        if qos == QOSLevel.QOS_0:
            self._received_messages.put_nowait(message)

        elif qos == QOSLevel.QOS_1:
            await self._send_puback(packet_id)
            self._received_messages.put_nowait(message)

        elif qos == QOSLevel.QOS_2:
            await self._send_pubrec(packet_id)
            self._incoming_messages[packet_id] = InflightMessage(
                message, InflightMessageState.WAIT_FOR_PUBREL)

    async def _handle_pubrel(self, reader: LimitedStreamReader) -> None:
        packet_id, = struct.unpack('!H', await reader.read(2))

        assert self._received_messages is not None

        inflight_message = self._incoming_messages.get(packet_id)
        if inflight_message is not None and inflight_message.state == InflightMessageState.WAIT_FOR_PUBREL:
            self._received_messages.put_nowait(inflight_message.message)
            self._incoming_messages.pop(packet_id)

    async def _handle_puback(self, reader: LimitedStreamReader) -> None:
        packet_id, = struct.unpack('!H', await reader.read(2))

        inflight_message = self._outgoing_messages.get(packet_id)
        if inflight_message is not None and inflight_message.state == InflightMessageState.WAIT_FOR_PUBACK:
            self._outgoing_messages.pop(packet_id)

        self._wakeup((PacketType.PUBACK, packet_id))

    async def _handle_pubrec(self, reader: LimitedStreamReader) -> None:
        packet_id, = struct.unpack('!H', await reader.read(2))

        await self._send_pubrel(packet_id)

        inflight_message = self._outgoing_messages.get(packet_id)
        if inflight_message is not None and inflight_message.state == InflightMessageState.WAIT_FOR_PUBREC:
            inflight_message.update_state(InflightMessageState.WAIT_FOR_PUBCOMP)

    async def _handle_pubcomp(self, reader: LimitedStreamReader) -> None:
        packet_id, = struct.unpack('!H', await reader.read(2))

        inflight_message = self._outgoing_messages.get(packet_id)
        if inflight_message is not None and inflight_message.state == InflightMessageState.WAIT_FOR_PUBCOMP:
            self._outgoing_messages.pop(packet_id)

        self._wakeup((PacketType.PUBCOMP, packet_id))

    async def _handle_pingresp(self) -> None:
        self._ping_response_received = True

    async def _read(self) -> None:
        assert self._reader is not None
        try:
            byte, = struct.unpack('!B', await self._reader.readexactly(1))
            packet_type = byte & 0xf0
            flags = byte & 0x0f
            multiplier = 1
            remaining_length = 0
            while True:
                byte, = struct.unpack('!B', await self._reader.readexactly(1))
                remaining_length += (byte & 0x7f) * multiplier
                if (byte & 0x80) == 0:
                    break

                else:
                    multiplier *= 128
                    if multiplier > 128 * 128 * 128:
                        raise MQTTClientProtocolError("Malformed remaining length")

            reader = LimitedStreamReader(self._reader, remaining_length)
            try:
                if packet_type == PacketType.CONNACK:
                    await self._handle_connack(reader)

                elif packet_type == PacketType.SUBACK:
                    await self._handle_suback(reader)

                elif packet_type == PacketType.UNSUBACK:
                    await self._handle_unsuback(reader)

                elif packet_type == PacketType.PUBLISH:
                    await self._handle_publish(flags, reader)

                elif packet_type == PacketType.PUBREL:
                    await self._handle_pubrel(reader)

                elif packet_type == PacketType.PUBACK:
                    await self._handle_puback(reader)

                elif packet_type == PacketType.PUBREC:
                    await self._handle_pubrec(reader)

                elif packet_type == PacketType.PUBCOMP:
                    await self._handle_pubcomp(reader)

                elif packet_type == PacketType.PINGRESP:
                    await self._handle_pingresp()

                else:
                    raise MQTTClientProtocolError("Unexpected control packet type")

            except ForbiddenReadingError as e:
                raise MQTTClientProtocolError("Unexpected end of packet") from e

            if reader:
                raise MQTTClientProtocolError("Not all packet data was consumed")

        except (ConnectionResetError, aio.IncompleteReadError) as e:
            raise MQTTClientConnectionLostError() from e

    async def _reading_loop(self) -> None:
        try:
            while True:
                await self._read()

        except aio.CancelledError:
            raise

        except Exception as e:
            aio.ensure_future(self._disconnect(e), loop=self._loop)

    async def _keepalive_mechanism(self, keepalive: int) -> None:
        await self._connected.wait()
        try:
            self._ping_response_received = False
            next_ping_time = time_func() + keepalive
            ping_response_no_later = None
            while True:
                await aio.sleep(1, loop=self._loop)
                now = time_func()
                if next_ping_time <= now:
                    await self._send_pingreq()
                    next_ping_time = now + keepalive
                    if ping_response_no_later is None:
                        ping_response_no_later = now + self._ping_response_waiting_timeout

                if ping_response_no_later is None:
                    continue

                if self._ping_response_received:
                    self._ping_response_received = False
                    ping_response_no_later = None

                elif ping_response_no_later <= now:
                    raise MQTTClientPingResponseWaitingTimedOutError()
        except aio.CancelledError:
            raise

        except Exception as e:
            aio.ensure_future(self._disconnect(e), loop=self._loop)

    async def _message_retry_check_actual(self, messages: ty.Dict[int, InflightMessage]) -> None:
        now = time_func()
        for packet_id, inflight_message in messages.items():
            if inflight_message.timestamp + self._message_retry_interval < now:
                if inflight_message.state in (
                        InflightMessageState.WAIT_FOR_PUBACK,
                        InflightMessageState.WAIT_FOR_PUBREC
                ):
                    await self._send_publish(inflight_message.message, packet_id, True)

                elif inflight_message.state == InflightMessageState.WAIT_FOR_PUBREL:
                    await self._send_pubrec(packet_id)

                elif inflight_message.state == InflightMessageState.WAIT_FOR_PUBCOMP:
                    await self._send_pubrel(packet_id)

                inflight_message.update_timestamp()

    async def _message_retry_check(self) -> None:
        await self._connected.wait()
        try:
            while True:
                await aio.sleep(1, loop=self._loop)
                await self._message_retry_check_actual(self._outgoing_messages)
                await self._message_retry_check_actual(self._incoming_messages)
        except aio.CancelledError:
            raise

        except Exception as e:
            aio.ensure_future(self._disconnect(e), loop=self._loop)
