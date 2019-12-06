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

Simple echo client:

.. code:: python

    import asyncio as aio

    from aio_mqtt import (
        QOSLevel,
        PublishableMessage,
        MQTTClient,
        MQTTClientError,
        MQTTClientChannelClosedError
    )


    async def handle_disconnects(client: MQTTClient) -> None:
        while True:
            try:
                await client.wait_disconnecting()
            except MQTTClientError:
                # Handle unusual disconnects here
                return

            else:
                # Gracefully disconnected
                break


    async def main(client: MQTTClient) -> None:
        try:
            await client.connect('localhost')
        except MQTTClientError:
            # Handle connection errors here
            return

        try:
            await client.subscribe(('in', QOSLevel.QOS_1))
        except MQTTClientError:
            # Handle subscription errors here
            return

        while True:
            try:
                message = await client.receive_message()
            except MQTTClientChannelClosedError:
                return

            try:
                await client.publish(PublishableMessage(topic_name='out', payload=message.payload, qos=QOSLevel.QOS_1))
            except MQTTClientError:
                return


    if __name__ == '__main__':
        loop = aio.new_event_loop()
        client = MQTTClient()
        try:
            loop.run_until_complete(aio.gather(handle_disconnects(client), main(client)))
        except KeyboardInterrupt:
            pass

        finally:
            loop.run_until_complete(client.disconnect())
            loop.close()

License
#######

Copyright 2019 Not Just A Toy Corp.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
