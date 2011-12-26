
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from lib.client import connect_discovery
from lib.client import DISCOVERY_HOST, DISCOVERY_PORT

import random

from discovery import Discovery, ttypes as o

def serve(host='127.0.0.1',port=9191):
    # setup the processors for each of the services
    from services.scraper import Scraper
    from services.requester import Requester

    # the requests handler
    from services.request_service import MatureRequestHandler
    handler = MatureRequestHandler()
    processor = Requester.Processor(handler)

    # the scraper handler
    from scraper_service import ScraperHandler
    handler = ScraperHandler()
    processor = Scraper.Processor(handler)

    transport = TSocket.TServerSocket(host,port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    print 'starting'
    server.serve()
    print 'done'


def serve_service(service, handler, host='127.0.0.1',port=None,
                  is_discovery=False):

    PORT_MIN = 9192
    PORT_MAX = 10999
    if not port and not is_discovery:
        # choose a random port
        port = random.randint(PORT_MIN,PORT_MAX)
    elif not port and is_discovery:
        # if we are discovery .. user our details
        port = DISCOVERY_PORT
        host = DISCOVERY_HOST

    processor = getattr(service,'Processor')(handler)
    transport = TSocket.TServerSocket(host,port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    print 'registering server: %s %s %s' % (
            service.__name__,host,port)

    # register our service w/ the discovery service
    if not is_discovery:
        with connect_discovery() as discovery_client:
            service_details = o.Service(
                host=host,
                port=port,
                name=service.__name__
            )
            discovery_client.register_service(service_details)

    print 'starting: %s %s %s' % (service,host,port)
    try:
        server.serve()
    finally:
        # tell the discovery service we're gone
        if not is_discovery:
            print 'removing service from discovery'
            with connect_discovery() as discovery_client:
                discovery_client.remove_service(service_details)

    print 'done'

if __name__ == '__main__':
    import sys
    n = sys.argv[1]
    print 'starting: %s' % n
    mn = 'services.%s' % n
    print 'module name: %s' % mn
    import_string = 'from %s import run as run_service' % mn
    exec(import_string)
    print 'running'
    run_service()
