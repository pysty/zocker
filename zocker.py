import argparse
import asyncio
from functools import partial
import json
import logging
import sys

from aiohttp import ClientSession, HttpProcessingError
from schema import Schema, SchemaError, Optional, Or
from websockets import server, InvalidState
import yaml
import zmq.asyncio

schema = Schema({
    'host': str,
    'pub': {
        'port': int
    },
    'pull': {
        'port': int
    },
    'websocket': {
        'port': int
    },
    Optional('auth'): Or(
        {'restapi': str}
    )
})

log = logging.getLogger(__file__)

context = zmq.asyncio.Context()


class AccessDenied(Exception):
    pass


def eprint(*args, fatal=True, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
    if fatal:
        exit(-1)


async def pub_server(config):
    bind_fmt = partial('tcp://{}:{}'.format, config['host'])

    pub = context.socket(zmq.PUB)
    pub.bind(bind_fmt(config['pub']['port']))

    pull = context.socket(zmq.PULL)
    pull.bind(bind_fmt(config['pull']['port']))

    while True:
        msg = await pull.recv()
        await pub.send(msg)


async def restapi_auth(websocket, config):
    if 'restapi' in config.get('auth', {}):
        cookies = websocket.request_headers.get('Cookie')
        url = config['auth']['restapi']
        headers = {'Cookie': cookies} if cookies else None

        with ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                content = (await response.content.read()).decode('utf-8')

                try:
                    response.raise_for_status()
                    if not json.loads(content).get('authenticated'):
                        raise ValueError
                except (AttributeError, ValueError, HttpProcessingError):
                    raise AccessDenied


async def handle_websocket(websocket, uri_path, config):
    try:
        await restapi_auth(websocket, config)
    except AccessDenied:
        log.info()

    topic = await websocket.recv()
    await websocket.send('ok')

    sub = context.socket(zmq.SUB)
    sub.setsockopt_string(zmq.SUBSCRIBE, topic)
    sub.connect('tcp://{}:{}'.format(config['host'], config['pub']['port']))

    while True:
        msg = await sub.recv()
        try:
            await websocket.send(msg.decode('utf-8'))
        except InvalidState:
            break


def get_config():
    arg_parser = argparse.ArgumentParser(
        description='Zocker - PUB/SUB pattern for websockets using ZeroMQ'
    )
    arg_parser.add_argument('--config', type=str, default='zockerconf.yaml')
    args = arg_parser.parse_args()

    try:
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        eprint('{}: {}: No such file'.format(sys.argv[0], args.config))

    try:
        # noinspection PyUnboundLocalVariable
        schema.validate(config)
    except SchemaError as e:
        eprint('{}: Configuration error: {}'.format(sys.argv[0], e))

    # pyzmq can't bind to ``localhost``
    if config['host'] == 'localhost':
        config['host'] = '127.0.0.1'

    return config


def main():
    config = get_config()

    loop = zmq.asyncio.ZMQEventLoop()
    asyncio.set_event_loop(loop)

    serve = server.serve(
        partial(handle_websocket, config=config),
        host=config['host'],
        port=config['websocket']['port']
    )

    asyncio.ensure_future(pub_server(config))
    asyncio.ensure_future(serve)
    loop.run_forever()


if __name__ == '__main__':
    main()
