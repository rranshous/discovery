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
    for service, lookup in global_transport_lookup.iteritems():
        for s, t in lookup.iteritems():
            t.close()
atexit.register(cleanup_transports)

@contextmanager
def connect_discovery():
    try:
        from lib.discovery import Discovery
    except ImportError:
        from discovery import Discovery

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
    try:
        from lib.discovery import Discovery
    except ImportError:
        from discovery import Discovery

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
def connect_reuse(service,host=None,port=None,rediscover=False):

    global endpoint_lookup
    global global_transport_lookup
    global global_client_lookup

    try:
        thread = threading.current_thread()
        if thread:
            thread = thread.ident
    except Exception, ex:
        print 'Exception identifying thread: %s' % ex
        thread = None

    # keep our lookup thread specific
    client_lookup = global_client_lookup.setdefault(thread,{})
    transport_lookup = global_transport_lookup.setdefault(thread,{})

    if rediscover and client_lookup.get(service):
        # refresh the client
        transport_lookup.get(service).close()
        del transport_lookup[service]
        del client_lookup[service]

    if not client_lookup.get(service):

        if not host or not port and service in endpoint_lookup:
            endpoint_list = endpoint_lookup.get(service,[])
            if endpoint_list:
                host, port = random.sample(endpoint_list,1)[0]

        if not host or not port:

            # if we didn't get a host / port use service
            # discovery to find it
            with connect_discovery() as c:
                print 'lookup up: %s' % service
                service_name = service.__name__.split('.')[-1]
                service_details = c.find_service(service_name)
                assert service_details, "Could not find service in discovery"
                port = service_details.port
                host = service_details.host
                endpoint_lookup.setdefault(service,[]).append((host,port))

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
            endpoint_lookup.setdefault(service,[]).append((host,port))

    transport = TSocket.TSocket(host,port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = getattr(service,'Client')(protocol)
    transport.open()
    yield client
    transport.close()



# TODO: close transports
class ThriftClientContext(object):

    # service => [(host,port)]
    endpoint_lookup = {}

    # service => thread_ident => transport
    transport_lookup = {}

    # service => thread_ident => client
    client_lookup = {}

    # votes against an endpoint
    endpoint_downvotes = {}

    # @ what point do we remove an endpoint ?
    downvote_threshold = 10 # TODO: make this smarter?
                            #       perhaps rate limiter?

    def __init__(self, service):

        # what service are we trying to connect to ?
        self.service = service

        # the transport / client currently in context
        self.transport = None
        self.client = None

    @classmethod
    def _get_endpoint(cls, service):
        """ returns an endpoint (host,port) for a given service.
            if none is found than it returns None, None.
            will attempt to return from cache """

        print 'lookup: %s' % cls.endpoint_lookup

        # pick a random endpoint
        endpoint_list = cls.endpoint_lookup.get(service,[(None,None)])
        host, port = random.sample(endpoint_list,1)[0]

        # if we didn't get an endpoint, look one up
        if not host:

            # connect to the discovery service and try and find endpoint with connect_discovery() as c:

            # if we didn't get a host / port use service
            # discovery to find it
            with connect_discovery() as c:
                print 'lookup up: %s' % service

                # get our service by name
                service_name = service.__name__.split('.')[-1]
                service_details = c.find_service(service_name)

                # found one !
                if service_details:

                    port = service_details.port
                    host = service_details.host

                    # note in lookup for the next chap
                    lookup = cls.endpoint_lookup
                    lookup.setdefault(service,[]).append((host,port))

        return host, port

    @staticmethod
    def _get_thread_ident():
        try:
            # grab our thread
            thread = threading.current_thread()

            # and get it's id
            if thread:
                thread_ident = thread.ident
                return thread_ident

        except Exception, ex:
            # TODO
            # SILENT PASS = BAD
            pass

        return None

    @classmethod
    def _create_client(cls, service, host, port):
        """ creates a new client instance """

        print 'creating client: %s %s %s' % (service,host,port)
        transport = TSocket.TSocket(host,port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = getattr(service,'Client')(protocol)
        client.endpoint = (host,port)
        transport.open()
        return client, transport

    def get_client(self):
        """ returns a client for the given service """

        # identify what thread we are in
        thread_ident = self._get_thread_ident()

        # see if we already have a client
        self.client = self.client_lookup.get(self.service,{}).get(thread_ident)

        # if we didn't get a client, than we are going
        # to need to create one

        if not self.client:

            # where should our client connect ?
            host, port = self._get_endpoint(self.service)

            # open up a new client
            self.client, self.transport = self._create_client(self.service,
                                                              host,port)

            # add to cache
            s_lookup = self.client_lookup.setdefault(self.service,{})
            s_lookup.setdefault(thread_ident,[]).append(self.client)

        return self.client


    def __enter__(self):
        """ returns client for service """
        return self.get_client()

    def __exit__(self, ex_type, ex_val, ex_tb):
        """
        raises a generic exception on behalf of thrift exceptions,
        tracks and reports endpoint issues
        """

        # if we got an exception, vote against that endpoint being good
        if ex_type:
            print 'ex_type: %s' % ex_type
            self._downvote_endpoint(self.client.endpoint, self.service)

            # remove the client from the lookup
            s_lookup = self.client_lookup.setdefault(self.service,{})
            s_lookup[self._get_thread_ident()].remove(self.client)

        # clear our active transport / client
        self.transport = None
        self.client = None

        # we want to raise a normal Exception with the thrift exception
        # wrapperd in it
        if ex_type:
            e =  Exception("caught service exception",
                            self.service, ex_val, ex_tb)
            e.__context__ = ex_tb
            raise e


    @classmethod
    def _downvote_endpoint(cls, endpoint, service=None):
        """ downloads the endpoint, removes it if
            it has too many votes """

        # how many votes against it does this endpoint have ?
        votes = cls.endpoint_downvotes.get(endpoint,0)

        # too high?
        if votes > cls.downvote_threshold:

            # remove the endpoint from our lookup
            cls._remove_endpoint(endpoint, service)

        # TODO: report teh downvote to the discovery service

        # uptick our votes against the endpoint
        votes += 1
        cls.endpoint_downvotes[endpoint] = votes

        return True

    @classmethod
    def _remove_endpoint(self, endpoint, service=None):
        """
        removes the endpoint from our lookup cache
        """

        # remove the endpoint, if we know the service
        # than we can get it immediately
        if service:

            # we should def find the endpoint in the lookup
            endpoints = self.endpoint_lookup.get(service)
            try:
                endpoints.remove(endpoint)
            except ValueError:
                raise Exception('Could not find endpoint in lookup')
        else:
            # if we don't know the service we have to search
            found = False
            for service, endpoints in self.endpoint_lookup.iteritems():
                try:
                    endpoints.remove(endpoint)
                    found = True
                    break
                except ValueException:
                    # not there
                    continue
            if not found:
                raise Exception('Could not find endpoint in lookup')

connect = connect_reuse
