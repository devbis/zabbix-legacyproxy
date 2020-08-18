import asyncio
import json
import logging
import os
import re
import struct


logger = logging.getLogger(__name__)
os.environ.setdefault('ZBX_SERVER', '127.0.0.1')
os.environ.setdefault('ZBX_PORT', '10051')

PROXY_ADDRESS = '0.0.0.0'
PROXY_PORT = 10051

ZABBIX_SERVER = os.environ['ZBX_SERVER']
ZABBIX_PORT = int(os.environ['ZBX_PORT'])

info_rx = re.compile(
    r"processed: (\d+); failed: (\d+); total: (\d+); seconds spent: ([\d.]+)]"
)
info_replacement = "Processed {} Failed {} Total {} Seconds spent {}}"


def packed2data(packed_data: bytes) -> bytes:
    header, flags, length = struct.unpack('<4sBQ', packed_data[:13])
    assert header == b'ZBXD'
    assert flags == 1
    assert length != 0
    (data, ) = struct.unpack('<%ds' % length, packed_data[13:13+length])
    return data


def data2packed(data: bytes) -> bytes:
    header_field = struct.pack('<4sBQ', b'ZBXD', 1, len(data))
    return header_field + data


class ZabbixLegacyClientProxy:
    @staticmethod
    def upgrade_request(data: bytes):
        """
        In request['request'] replace 'agent data' with 'sender data'
        """
        upgraded = False
        content = json.loads(data)
        if content['request'] == 'agent data':
            content['request'] = 'sender data'
            upgraded = True

        if upgraded:
            return json.dumps(content).encode('utf-8')
        return data

    @staticmethod
    def upgrade_response(data: bytes):
        """
        Fix response['info']
        """

        upgraded = False
        try:
            content = json.loads(data)
        except ValueError:
            return data
        if isinstance(content, dict):
            r = info_rx.match(content.get('info'))
            if r:
                content['info'] = info_replacement.format(
                    r.group(1),
                    r.group(2),
                    r.group(3),
                    r.group(4),
                )
                upgraded = True
        if upgraded:
            return json.dumps(content).encode('utf-8')
        return data

    @classmethod
    async def request_replacer(cls, reader, writer):
        buffer = b''
        try:
            while not reader.at_eof():
                buffer += await reader.read(2048)
            try:
                data = packed2data(buffer)
                data = cls.upgrade_request(data)
            except (IndexError, ValueError):
                logger.exception("Cannot upgrade request {}".format(buffer))
                data = buffer
            writer.write(data2packed(data))
        finally:
            writer.close()

    @classmethod
    async def response_replacer(cls, reader, writer):
        buffer = b''
        try:
            while not reader.at_eof():
                buffer += await reader.read(2048)
            try:
                data = cls.upgrade_response(buffer)
            except (IndexError, ValueError):
                logger.exception("Cannot upgrade response {}".format(buffer))
                data = buffer
            writer.write(data)
        finally:
            writer.close()

    @classmethod
    async def transparent_pipe(cls, reader, writer):
        try:
            while not reader.at_eof():
                writer.write(await reader.read(2048))
        finally:
            writer.close()

    async def handle_client(self, local_reader, local_writer):
        try:
            remote_reader, remote_writer = await asyncio.open_connection(
                ZABBIX_SERVER,
                ZABBIX_PORT,
            )
            request_pipe = self.request_replacer(local_reader, remote_writer)
            response_pipe = self.response_replacer(remote_reader, local_writer)
            await asyncio.gather(request_pipe, response_pipe)
        finally:
            local_writer.close()

    def run(self):
        return asyncio.start_server(
            self.handle_client,
            PROXY_ADDRESS,
            PROXY_PORT,
            reuse_port=True,
        )


def main():
    loop = asyncio.get_event_loop()

    proxy = ZabbixLegacyClientProxy()
    server = loop.run_until_complete(proxy.run())

    # Serve requests until Ctrl+C is pressed
    print('Serving on {}:{}'.format(*server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == '__main__':
    main()
