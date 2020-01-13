***********
MQTT client
***********

About
#####

Asynchronous MQTT client for 3.1.1 protocol version.

Installation
############

Recommended way (via pip):

.. code:: bash

    $ pip install aio-mqtt

Example
#######

Simple echo server:

.. code:: python

    import asyncio as aio
    import logging
    import typing as ty

    import aio_mqtt

    logger = logging.getLogger(__name__)


    class EchoServer:

        def __init__(
                self,
                reconnection_interval: int = 10,
                loop: ty.Optional[aio.AbstractEventLoop] = None
        ) -> None:
            self._reconnection_interval = reconnection_interval
            self._loop = loop or aio.get_event_loop()
            self._client = aio_mqtt.Client(loop=self._loop)
            self._tasks = [
                self._loop.create_task(self._connect_forever()),
                self._loop.create_task(self._handle_messages())
            ]

        async def close(self) -> None:
            for task in self._tasks:
                if task.done():
                    continue
                task.cancel()
                try:
                    await task
                except aio.CancelledError:
                    pass
            if self._client.is_connected():
                await self._client.disconnect()

        async def _handle_messages(self) -> None:
            async for message in self._client.delivered_messages('in'):
                while True:
                    try:
                        await self._client.publish(
                            aio_mqtt.PublishableMessage(
                                topic_name='out',
                                payload=message.payload,
                                qos=aio_mqtt.QOSLevel.QOS_1
                            )
                        )
                    except aio_mqtt.ConnectionClosedError as e:
                        logger.error("Connection closed", exc_info=e)
                        await self._client.wait_for_connect()
                        continue

                    except Exception as e:
                        logger.error("Unhandled exception during echo message publishing", exc_info=e)

                    break

        async def _connect_forever(self) -> None:
            while True:
                try:
                    connect_result = await self._client.connect('localhost')
                    logger.info("Connected")

                    await self._client.subscribe(('in', aio_mqtt.QOSLevel.QOS_1))

                    logger.info("Wait for network interruptions...")
                    await connect_result.disconnect_reason
                except aio.CancelledError:
                    raise

                except aio_mqtt.AccessRefusedError as e:
                    logger.error("Access refused", exc_info=e)

                except aio_mqtt.ConnectionLostError as e:
                    logger.error("Connection lost. Will retry in %d seconds", self._reconnection_interval, exc_info=e)
                    await aio.sleep(self._reconnection_interval, loop=self._loop)

                except aio_mqtt.ConnectionCloseForcedError as e:
                    logger.error("Connection close forced", exc_info=e)
                    return

                except Exception as e:
                    logger.error("Unhandled exception during connecting", exc_info=e)
                    return

                else:
                    logger.info("Disconnected")
                    return


    if __name__ == '__main__':
        logging.basicConfig(
            level='DEBUG'
        )
        loop = aio.new_event_loop()
        server = EchoServer(reconnection_interval=10, loop=loop)
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

        finally:
            loop.run_until_complete(server.close())
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

License
#######

Copyright 2019-2020 Not Just A Toy Corp.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
