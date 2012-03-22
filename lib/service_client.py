from contextlib import contextmanager
import atexit

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import random

import threading

DISCOVERY_HOST = '127.0.0.1'
DISCOVERY_PORT = 9191

global_client_lookup = {}
global_transport_lookup = {}
endpoint_lookup = {}

# when we exit clean up our
def cleanup_transports():
    for service, transport in transport_lookup.iteritems():
        transport.close()
atexit.register(cleanup_transports)

@contextmanager
def connect_discovery():
    from lib.discovery import Discovery

    host = DISCOVERY_HOST
    port = DISCOVERY_PORT
    service = Discovery

    if not client_lookup.get(service):
        transport = TSocket.TSocket(host,port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = getattr(service,'Client')(protocol)
        client_lookup[service] = client
        transport_lookup[service] = transport
        transport.open()

    # TODO: if there is a connection issue
    # w/ the client re-create it

    yield client_lookup.get(service)

@contextmanager
def connect_discovery():
    from lib.discovery import Discovery

    host = DISCOVERY_HOST
    port = DISCOVERY_PORT
    service = Discovery

    transport = TSocket.TSocket(host,port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = getattr(service,'Client')(protocol)
    transport.open()
    yield client
    transport.close()


@contextmanager
def connect_reuse(service,host=None,port=None,rediscover=True):

    global endpoint_lookup
    global global_transport_lookup
    global global_client_lookup

    thread = threading.current_thread()
    if thread:
        thread = thread.get_ident()

    # keep our lookup thread specific
    client_lookup = global_client_lookup.get(thread,{})
    transport_lookup = global_transport_lookup.get(thread,{})

    if rediscover and client_lookup.get(service):
        # refresh the client
        transport_lookup.get(service).close()
        del transport_lookup[service]
        del client_lookup[service]

    if not client_lookup.get(service):

        if not host or not port and service in endpoint_lookup:
            endpoint_list = endpoint_lookup.get(service)
            if endpoint_list:
                host, port = random.sample(endpoint_list,1)[0]

        if not host or not port:

            # if we didn't get a host / port use service
            # discovery to find it
            with connect_discovery() as c:
                service_name = service.__name__.split('.')[-1]
                service_details = c.find_service(service_name)
                assert service_details, "Could not find service in discovery"
                port = service_details.port
                host = service_details.host

            transport = TSocket.TSocket(host,port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = getattr(service,'Client')(protocol)
            client_lookup[service] = client
            transport_lookup[service] = transport
            transport.open()

    yield client_lookup.get(service)

@contextmanager
def connect_no_reuse(service,host=None,port=None):

    global endpoint_lookup

    if not host or not port and service in endpoint_lookup:
        endpoint_list = endpoint_lookup.get(service)
        if endpoint_list:
            host, port = random.sample(endpoint_list,1)[0]

    if not host or not port:
        with connect_discovery() as c:
            service_name = service.__name__.split('.')[-1]
            service_details = c.find_service(service_name)
            assert service_details, "Could not find service in discovery"
            port = service_details.port
            host = service_details.host

    transport = TSocket.TSocket(host,port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = getattr(service,'Client')(protocol)
    transport.open()
    yield client
    endpoint_lookup.setdefault(service,[]).append((host,port))
    transport.close()


connect = connect_no_reuse
