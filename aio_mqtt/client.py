# Copyright 2019-2020 Not Just A Toy Corp.
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

import asyncio as aio
import collections
import datetime
import enum
import struct
import sys
import time
import typing as ty
import uuid
from ssl import SSLContext

from .constants import *
from .enums import *
from .exceptions import *
from .utils import *

__all__ = (
    'PublishableMessage',
    'DeliveredMessage',
    'ConnectResult',
    'Client',
)

try:
    # Use monotonic clock if available
    time_func = time.monotonic
except AttributeError:
    time_func = time.time


class PublishableMessage:

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


class DeliveredMessage(ty.NamedTuple):
    topic_name: str
    payload: bytes
    dup: bool
    qos: QOSLevel
    retain: bool
    timestamp: datetime.datetime


class InflightMessageState(enum.Enum):
    WAIT_FOR_PUBACK = enum.auto()
    WAIT_FOR_PUBREC = enum.auto()
    WAIT_FOR_PUBREL = enum.auto()
    WAIT_FOR_PUBCOMP = enum.auto()


T = ty.TypeVar('T', PublishableMessage, DeliveredMessage)


class InflightMessage(ty.Generic[T]):

    __slots__ = ('message', 'state', 'timestamp')

    message: T

    def __init__(
            self,
            message: T,
            state: InflightMessageState,
            timestamp: ty.Optional[float] = None
    ) -> None:
        self.message = message
        self.state = state
        if timestamp is None:
            timestamp = time_func()
        self.timestamp = timestamp

    def update_state(self, new_state: InflightMessageState) -> None:
        self.state = new_state
        self.update_timestamp()

    def update_timestamp(self, new_timestamp: ty.Optional[float] = None) -> None:
        self.timestamp = new_timestamp or time_func()


def packet_ids() -> ty.Generator[int, None, None]:
    packet_id = 1
    while True:
        yield packet_id
        packet_id = packet_id + 1 if packet_id <= MAXIMUM_PACKET_ID else 1


class ConnectResult(ty.NamedTuple):
    client_id: str
    session_present: bool
    disconnect_reason: aio.Future


class Client:

    def __init__(
            self,
            message_retry_interval: int = 20,
            ping_response_waiting_timeout: int = 20,
            client_id_prefix: ty.Optional[str] = None,
            loop: ty.Optional[aio.AbstractEventLoop] = None
    ) -> None:
        if message_retry_interval < 1:
            raise ValueError("Message retry interval must be >= 1")
        self._message_retry_interval = message_retry_interval
        if ping_response_waiting_timeout < 1:
            raise ValueError("Ping response waiting timeout must be >= 1")
        self._ping_response_waiting_timeout = ping_response_waiting_timeout
        self._client_id_prefix = client_id_prefix
        self._loop = loop or aio.get_event_loop()
        self._connected = False
        self._waiters: ty.DefaultDict[ty.Hashable, ty.Set[aio.Future]] = collections.defaultdict(set)
        self._tasks: ty.List[aio.Task] = []
        self._packet_ids = packet_ids()
        self._incoming_messages: ty.Dict[int, InflightMessage[DeliveredMessage]] = {}
        self._outgoing_messages: ty.Dict[int, InflightMessage[PublishableMessage]] = {}
        self._reader: ty.Optional[aio.StreamReader] = None
        self._writer: ty.Optional[aio.StreamWriter] = None
        self._drain_lock = aio.Lock(
            **({"loop": loop} if sys.version_info[:2] < (3, 8) else {})
        )
        self._connection_lock = aio.Lock(
            **({"loop": loop} if sys.version_info[:2] < (3, 8) else {})
        )
        self._ping_response_received = False
        self._consumer_queues: TopicMatcher[ty.Set[aio.Queue[DeliveredMessage]]] = TopicMatcher()
        self._disconnect_reason: ty.Optional[aio.Future] = None

    async def connect(
            self,
            host: str,
            port: int = 1883,
            ssl: ty.Optional[ty.Union[bool, SSLContext]] = None,
            keepalive: int = 60,
            client_id: ty.Optional[str] = None,
            clean_session: bool = True,
            will_message: ty.Optional[PublishableMessage] = None,
            username: ty.Optional[str] = None,
            password: ty.Optional[str] = None
    ) -> ConnectResult:
        if len(host) == 0:
            raise ValueError("Invalid host")

        if port <= 0:
            raise ValueError("Invalid port number")

        if keepalive < 0:
            raise ValueError("Keepalive must be >= 0")

        if not client_id and not clean_session:
            raise ValueError("A client id must be provided if clean session is False")

        async with self._connection_lock:
            if self._connected:
                raise AlreadyConnectedError()

            client_id = client_id or '%s%s' % (self._client_id_prefix, uuid.uuid4().hex)

            try:
                self._reader, self._writer = await aio.open_connection(
                    host=host, port=port, loop=self._loop, ssl=ssl)
            except OSError as e:
                raise ConnectFailedError() from e

            try:
                await self._send_connect(
                    keepalive=keepalive,
                    client_id=client_id,
                    clean_session=clean_session,
                    will_message=will_message,
                    username=username,
                    password=password
                )

                self._tasks.append(self._loop.create_task(self._reading_task()))
                self._tasks.append(self._loop.create_task(self._message_retry_checking_task()))
                if keepalive > 0:
                    self._tasks.append(self._loop.create_task(self._keepalive_mechanism_task(keepalive)))

                for task in self._tasks:
                    task.add_done_callback(self._task_done_callback)

                session_present = await self._wait(PacketType.CONNACK)

                if clean_session:
                    self._clean_session()
            except Exception:
                self._disconnect()
                raise

            self._connected = True

            self._wakeup('connect')

            future = self._loop.create_future()
            self._disconnect_reason = future

            return ConnectResult(
                client_id=client_id,
                session_present=session_present,
                disconnect_reason=future
            )

    async def disconnect(self) -> None:
        async with self._connection_lock:
            self._raise_if_not_connected()

            try:
                await self._send_disconnect()
            except Exception:
                self._disconnect()
                raise

            self._disconnect()

    async def subscribe(self, *topics: ty.Tuple[str, QOSLevel]) -> ty.Tuple[SubscribeReturnCode, ...]:
        self._raise_if_not_connected()

        if not topics:
            raise ValueError("Topics is empty")

        if any(not TOPIC_FILTER_REGEX.match(topic_filter) or len(topic_filter) > MAXIMUM_TOPIC_LENGTH
               for topic_filter, _ in topics):
            raise ValueError("Invalid topic filter")

        packet_id = next(self._packet_ids)

        await self._send_subscribe(topics, packet_id)

        return await self._wait((PacketType.SUBACK, packet_id))

    async def unsubscribe(self, *topic_filters: str) -> None:
        self._raise_if_not_connected()

        if any(not topic_filter or len(topic_filter) > MAXIMUM_TOPIC_LENGTH for topic_filter in topic_filters):
            raise ValueError("Invalid topic filter")

        packet_id = next(self._packet_ids)

        await self._send_unsubscribe(topic_filters, packet_id)

        await self._wait((PacketType.UNSUBACK, packet_id))

    async def publish(self, message: PublishableMessage, nowait: bool = False) -> None:
        self._raise_if_not_connected()

        packet_id = next(self._packet_ids)

        await self._send_publish(message, packet_id)

        try:
            if message.qos == QOSLevel.QOS_1:
                self._outgoing_messages[packet_id] = InflightMessage(
                    message, InflightMessageState.WAIT_FOR_PUBACK)

                if not nowait:
                    await self._wait((PacketType.PUBACK, packet_id))

            elif message.qos == QOSLevel.QOS_2:
                self._outgoing_messages[packet_id] = InflightMessage(
                    message, InflightMessageState.WAIT_FOR_PUBREC)

                if not nowait:
                    await self._wait((PacketType.PUBCOMP, packet_id))
        except aio.CancelledError:
            self._outgoing_messages.pop(packet_id, None)
            raise

    def is_connected(self) -> bool:
        return self._connected

    async def delivered_messages(self, topic_filter: str = '#') -> ty.AsyncGenerator[DeliveredMessage, None]:
        queues = self._consumer_queues.get(topic_filter)
        if queues is None:
            queues = set()
            self._consumer_queues[topic_filter] = queues
        queue: aio.Queue[DeliveredMessage] = aio.Queue(loop=self._loop)
        queues.add(queue)
        try:
            while True:
                message = await queue.get()
                try:
                    yield message
                finally:
                    queue.task_done()
        finally:
            queues.discard(queue)
            if not queues:
                try:
                    del self._consumer_queues[topic_filter]
                except KeyError:
                    pass

    async def wait_for_connect(self) -> None:
        if self._connected:
            return

        await self._wait('connect', cancel_previous=False)

    async def wait_for_disconnect(self) -> None:
        if not self._connected:
            return

        await self._wait('disconnect', cancel_previous=False)

    def _raise_if_not_connected(self) -> None:
        if self._connected:
            return

        raise ConnectionClosedError()

    async def _wait(self, key: ty.Hashable, cancel_previous: bool = True) -> ty.Any:
        waiters = self._waiters[key]

        if cancel_previous:
            for future in waiters:
                if not future.done():
                    future.cancel()

        future = self._loop.create_future()
        waiters.add(future)
        try:
            return await future
        finally:
            waiters.discard(future)

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

        for future in waiters:
            if future.done():
                continue

            if result is not None:
                future.set_result(result)

            elif exception is not None:
                future.set_exception(exception)

            else:
                future.set_result(None)

    def _wakeup_all(
            self,
            result: ty.Optional[ty.Any] = None,
            exception: ty.Optional[BaseException] = None
    ) -> None:
        for key in self._waiters.keys():
            self._wakeup(key, result=result, exception=exception)

    def _task_done_callback(self, future: aio.Future) -> None:
        exception = None
        try:
            exception = future.exception()
        except aio.CancelledError:
            pass

        if exception is not None:
            self._disconnect(exception=exception)

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
            raise ConnectionLostError() from e

    def _disconnect(self, exception: ty.Optional[BaseException] = None) -> None:
        if not self._connected:
            return

        self._connected = False

        for task in self._tasks:
            if not task.done():
                task.cancel()
        self._tasks.clear()

        if self._writer is not None:
            try:
                self._writer.close()
            except:  # noqa: E722
                pass
            self._writer = None

        if self._reader is not None:
            self._reader = None

        if self._disconnect_reason is not None:
            if not self._disconnect_reason.done():
                if exception is not None:
                    self._disconnect_reason.set_exception(exception)

                else:
                    self._disconnect_reason.set_result(None)

            self._disconnect_reason = None

        self._wakeup('disconnect')

        if exception is None:
            exception = ConnectionCloseForcedError()

        self._wakeup_all(exception=exception)

    def _maybe_put_delivered_message(self, message: DeliveredMessage) -> None:
        for queues in self._consumer_queues.iter_match(message.topic_name):
            for queue in queues:
                queue.put_nowait(message)

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

    async def _send_connect(
            self,
            *,
            keepalive: int,
            client_id: str,
            clean_session: bool,
            will_message: ty.Optional[PublishableMessage] = None,
            username: ty.Optional[str] = None,
            password: ty.Optional[str] = None
    ) -> None:
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
            self._wakeup(PacketType.CONNACK, result=session_present)

        else:
            self._wakeup(PacketType.CONNACK, exception=AccessRefusedError(
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

        message = DeliveredMessage(
            topic_name=topic_name,
            payload=payload,
            dup=bool(dup),
            qos=QOSLevel(qos),
            retain=bool(retain),
            timestamp=datetime.datetime.utcnow()
        )

        if qos == QOSLevel.QOS_0:
            self._maybe_put_delivered_message(message)

        elif qos == QOSLevel.QOS_1:
            await self._send_puback(packet_id)
            self._maybe_put_delivered_message(message)

        elif qos == QOSLevel.QOS_2:
            await self._send_pubrec(packet_id)
            self._incoming_messages[packet_id] = InflightMessage(
                message, InflightMessageState.WAIT_FOR_PUBREL)

    async def _handle_pubrel(self, reader: LimitedStreamReader) -> None:
        packet_id, = struct.unpack('!H', await reader.read(2))

        inflight_message = self._incoming_messages.get(packet_id)
        if inflight_message is not None and inflight_message.state == InflightMessageState.WAIT_FOR_PUBREL:
            self._maybe_put_delivered_message(inflight_message.message)
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
                        raise ProtocolError("Malformed remaining length")

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
                    raise ProtocolError("Unexpected control packet type")

            except ForbiddenReadingError as e:
                raise ProtocolError("Unexpected end of packet") from e

            if reader:
                raise ProtocolError("Not all packet data was consumed")

        except (ConnectionResetError, aio.IncompleteReadError) as e:
            raise ConnectionLostError() from e

    async def _reading_task(self) -> None:
        while True:
            await self._read()

    async def _keepalive_mechanism_task(self, keepalive: int) -> None:
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
                raise ServerDiedError()

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

    async def _message_retry_checking_task(self) -> None:
        while True:
            await aio.sleep(1, loop=self._loop)
            await self._message_retry_check_actual(self._outgoing_messages)
            await self._message_retry_check_actual(self._incoming_messages)
