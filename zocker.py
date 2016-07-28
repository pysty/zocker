import argparse
import asyncio
from functools import partial

from websockets import server
import zmq.asyncio

context = zmq.asyncio.Context()


async def pub_server(host, pub_port, pull_port):
    bind_fmt = partial('tcp://{}:{}'.format, host)

    pub = context.socket(zmq.PUB)
    pub.bind(bind_fmt(pub_port))

    pull = context.socket(zmq.PULL)
    pull.bind(bind_fmt(pull_port))

    while True:
        msg = await pull.recv()
        await pub.send(msg)


async def handle_websocket(websocket, uri_path, pub_host, pub_port):
    sub = context.socket(zmq.SUB)
    sub.setsockopt_string(zmq.SUBSCRIBE, '')
    sub.connect('tcp://{}:{}'.format(pub_host, pub_port))

    while True:
        msg = await sub.recv()
        await websocket.send(msg.decode('utf-8'))


def main():
    arg_parser = argparse.ArgumentParser(
        description='Zocker - PUB/SUB pattern for websockets using ZeroMQ'
    )
    arg_parser.add_argument('host', type=str)
    arg_parser.add_argument('websocket_port', type=int)
    arg_parser.add_argument('pub_port', type=int)
    arg_parser.add_argument('pull_port', type=int)
    args = arg_parser.parse_args()

    if args.host == 'localhost':
        args.host = '127.0.0.1'

    loop = zmq.asyncio.ZMQEventLoop()
    asyncio.set_event_loop(loop)

    serve = server.serve(
        partial(handle_websocket,
                pub_host=args.host,
                pub_port=args.pub_port),
        host=args.host,
        port=args.websocket_port
    )

    asyncio.ensure_future(
        pub_server(args.host,
                   args.pub_port,
                   args.pull_port)
    )
    asyncio.ensure_future(serve)
    loop.run_forever()


if __name__ == '__main__':
    main()

