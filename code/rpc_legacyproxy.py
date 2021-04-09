import json
import logging
import os
import re

import aiohttp
from aiohttp import (ClientConnectionError, ClientOSError, ClientResponse,
                     hdrs, web)
from multidict import CIMultiDict

os.environ.setdefault('ZBX_API', 'http://127.0.0.1')

ZABBIX_RPC_URL = os.environ['ZBX_API']
PROXY_PORT = 80

logger = logging.getLogger(__name__)

timedelta_rx = re.compile(r'(?P<value>\d+)(?P<suffix>[smhdw]?)')


def fix_timedelta(value):
    if not isinstance(value, str):
        return value
    r = timedelta_rx.match(value)
    if not r:
        return value
    try:
        value = int(r.group('value'))
        suffix = r.group('suffix')
        multipliers = {
            'm': 60,
            'h': 3600,
            'd': 1,  # day-sized in legacy zabbix
            'w': 7,
            # 'd': 86400,
            # 'w': 604800,
        }
        return str(value * multipliers.get(suffix, 1))
    except ValueError:
        return value


async def fix_json_response(response: ClientResponse) -> bytes:
    body = await response.read()
    logger.debug('  ..with data {}'.format(body))
    try:
        content = json.loads(body)
    except ValueError:
        logger.error("Not a JSON response: {}".format(body))
        return body

    if not isinstance(content, dict):
        logger.warning("Content is not a dict: {}".format(content))
        return body

    result = content.get('result')
    if not isinstance(result, list):
        logger.debug("Result is not a list: {}".format(result))
        return body

    fixed_result = []
    for item in result:
        fixed_result.append({
            key: fix_timedelta(value)
            for key, value in item.items()
        })
    content['result'] = fixed_result
    return json.dumps(content, ensure_ascii=False).encode('utf-8')


async def get_fixed_request_content(request: web.Request):
    try:
        content = await request.json()
    except ValueError:
        return request.content
    try:
        method = content.get('method')
    except AttributeError:
        return request.content

    if method in ['user.authenticate', 'user.login']:
        content['method'] = 'user.login'
        content.pop('auth', None)
    if method in ['usermacro.get']:
        params = content.get('params', {})
        if params.get('output') == 'refer':
            params['output'] = {
                'usermacro.get': [
                    # 'globalmacroid',
                    # 'hostid',
                    'hostmacroid',
                    # 'macro',
                    # 'value',
                    # 'description',
                    # 'type',
                ],
            }.get(method, 'extend')
        content['params'] = params
    elif method in ['host.get']:
        params = content.get('params', {})
        select_macros = params.pop('select_macros', None)
        if select_macros:
            params['selectMacros'] = select_macros
        select_groups = params.pop('select_groups', None)
        if select_groups:
            params['selectGroups'] = select_groups

        content['params'] = params

    return json.dumps(content, ensure_ascii=False)


async def handler_path(request: web.Request):
    path = request.raw_path
    upstream_url = '{}{}'.format(ZABBIX_RPC_URL.rstrip('/?'), path)
    req_headers = CIMultiDict(request.headers)
    for header in [
        hdrs.HOST,
        hdrs.CONNECTION,
        hdrs.COOKIE,
        hdrs.CONTENT_LENGTH,
        hdrs.CONTENT_ENCODING,
    ]:
        if header in req_headers:
            del req_headers[header]
    data = await get_fixed_request_content(request)
    async with aiohttp.ClientSession() as client:
        try:
            logger.debug('Fetch {}'.format(upstream_url))
            async with client.request(
                request.method,
                upstream_url,
                data=data,
                headers=req_headers,
            ) as resp:
                if 200 <= resp.status < 300:
                    body = await fix_json_response(resp)
                else:
                    body = await resp.read()
                resp_headers = CIMultiDict(resp.headers)
                for header in [
                    hdrs.CONTENT_LENGTH,
                    hdrs.CONTENT_ENCODING,
                    hdrs.TRANSFER_ENCODING,
                ]:
                    if header in resp_headers:
                        del resp_headers[header]
                return web.Response(
                    body=body,
                    status=resp.status,
                    headers=resp_headers,
                )
        except (ClientOSError, ClientConnectionError):
            logger.exception(
                "Cannot connect to upstream {}".format(upstream_url),
            )
            return web.Response(
                body=b'{}',
                status=500,
                content_type='application/json',
            )


def main():
    logging.basicConfig(level=logging.DEBUG)

    app = web.Application()
    app.router.add_route(hdrs.METH_ANY, r'/{path:.*}', handler_path)
    app.router.add_route(hdrs.METH_ANY, r'', handler_path)
    web.run_app(app, port=PROXY_PORT)


if __name__ == '__main__':
    main()
