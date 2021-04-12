import time
import socket

from threading import Thread
from collections import namedtuple
from collections.abc import Sequence

from .utils import repeated_execution, wait
from .__stx_etx import *

AGVMessage = namedtuple('AGVMessage', ['server_id', 'server_address', 'agv_id', 'tag', 'value'])

__all__ = ['VisionSocketClient', 'VisionSocketClientPool']


class VisionSocketClient:
    __message_length = 14

    def __init__(self, server_id, server_address, port, agv_id=None, timeout=60, msg_handler=None):
        self.server_id = server_id
        self.server_address = server_address
        self.port = port
        self.agv_id = agv_id
        self.timeout = timeout
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.msg_handler = msg_handler
        self.listen_thread = Thread(target=self.listen_loop)
        self._last_handshake_time = None
        self._connected = False
        self._running = False
        self.reconnect()

    @repeated_execution(5)
    def reconnect(self):
        if not self._connected:
            try:
                self.s.connect((self.server_address, self.port))
                self._connected = True
                self._last_handshake_time = time.time()
                print(f'Connected to ({self.server_address}, {self.port})')
            except socket.error:
                pass
                # print(f'[SocketError] Unable to connect to ({self.server_address}, {self.port})')
        else:
            # print(self.server_address, time.time() - self._last_handshake_time)
            if time.time() - self._last_handshake_time > self.timeout:
                self._connected = False
                print(f'[SocketError] Lost connection to ({self.server_address}, {self.port})')

    def send(self, content, flags=0):
        print("sent", content)
        ret = self.s.send(content, flags)
        self._last_handshake_time = time.time()
        return ret

    def receive(self, buffer=1024, flags=0):
        return self.s.recv(buffer, flags).decode()

    @staticmethod
    def add_checksum(cmd):
        if not isinstance(cmd, bytes): cmd = bytes(cmd, 'utf-8')
        return cmd + bytes(hex(sum(cmd))[-1], 'utf-8')

    @staticmethod
    def check_checksum(cmd):
        if not isinstance(cmd, bytes): cmd = bytes(cmd, 'utf-8')
        return VisionSocketClient.add_checksum(cmd[:-1]) == cmd

    def listen_loop(self):
        while self._running:
            if not self._connected:
                wait(1)
                continue
            ret = self.s.recv(1024)
            self._last_handshake_time = time.time()
            if len(ret) != self.__message_length or not (ret[:1] == STX and ret[-1:] == ETX): return
            ret = ret[1:-1]
            self.agv_id = int(ret[1:4])
            if ret[4:5] != b'H' and self.check_checksum(ret):
                try:
                    msg = AGVMessage(server_id=self.server_id, server_address=self.server_address,
                                     agv_id=int(ret[1:4]), tag=ret[4:7].decode(), value=int(ret[7:11]))
                    if self.msg_handler is not None:
                        self.msg_handler(msg)
                except:
                    print('[SocketError] Unresolvable message:', ret)

    def start_listening(self):
        self._running = True
        self.listen_thread.start()

    def stop_listening(self):
        self._running = False
        self.listen_thread.join()


class VisionSocketClientPool:

    def __init__(self, server_addresses, server_ports):
        if not isinstance(server_ports, Sequence):
            server_ports = [server_ports for _ in range(len(server_addresses))]
        self.server_addresses = server_addresses
        self.server_ports = server_ports
        self.clients = []
        self.listeners = []
        for server_i, (server_address, server_port) in enumerate(zip(server_addresses, server_ports)):
            self.clients.append(
                VisionSocketClient(server_i, server_address, server_port,
                                   msg_handler=self.handle_msg))

    def __len__(self):
        return len(self.clients)

    def __iter__(self):
        return iter(self.clients)

    def __getitem__(self, item):
        return self.clients[item]

    def add_listener(self, listener):
        self.listeners.append(listener)

    def handle_msg(self, agv_msg):
        for listener in self.listeners:
            listener.handle_msg(agv_msg)

    def start_listening(self):
        for client in self.clients:
            client.start_listening()


if __name__ == "__main__":
    client_pool = VisionSocketClientPool(
        server_addresses=["107.115.223.52"], server_ports=4001
    )
    client_pool.start_listening()
