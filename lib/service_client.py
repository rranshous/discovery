from contextlib import contextmanager
import atexit

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

DISCOVERY_HOST = '127.0.0.1'
DISCOVERY_PORT = 9191

client_lookup = {}
transport_lookup = {}

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
def connect(service,host=None,port=None,rediscover=True):

    if rediscover and client_lookup.get(service):
        # refresh the client
        transport_lookup.get(service).close()
        del transport_lookup[service]
        del client_lookup[service]

    if not client_lookup.get(service):
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
