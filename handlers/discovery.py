from tgen.discovery import Discovery, ttypes as o

from redis import Redis
import random

class DiscoveryHandler(object):

    def __init__(self, redis_host='127.0.0.1'):
        self.redis_host = redis_host
        self.rc = Redis(self.redis_host)

        # we are going to store the service records in redis

        # port record
        # discovery:<host>:<service name> = [<port>]

        # host record
        # discovery:<service name> = [<host>]

        # we are also going to keep a list of all the services currently
        # registered as discovery:service_list = ['<servicename>:<host>:<port>']
        # so we know if a service is re-registering

        # TODO: in the case that a client can't hit the service after
        # we've told it that it exists i'd like the client to be able
        # to "vote" that the service is down. a certiain # of votes / time
        # and we count it as dead.


    def get_service_key(self, service_name=None, host=None,
                              port=None, service=None):
        name = service_name or service.name
        port = port or service.port
        host = host or service.host
        k = 'discovery:%s:%s:%s' % (name,host,port)
        print 'get_service_key: %s' % k
        return k

    def get_host_key(self, service_name=None, service=None):
        name = service_name or service.name
        k = 'discovery:%s' % name
        print 'get host key: %s' % k
        return k

    def get_port_key(self, service_name=None, host=None, service=None):
        host = host or service.host
        name = service_name or service.name
        k = 'discovery:%s:%s' % (host,name)
        print 'get port key: %s' % k
        return k


    def register_service(self, service):
        """
        starts tracking new instance of service
        """

        print 'registering service: %s' % service

        # try adding the new service to the service list
        if self.rc.sadd('discovery:service_list',
                        self.get_service_key(service=service)):

            # update hte host list
            self.rc.sadd(self.get_host_key(service=service),service.host)

            # update the port list
            self.rc.sadd(self.get_port_key(service=service),service.port)

            return True

        return False


    def remove_service(self, service):
        """
        removes service from tracking
        """

        print 'removing service: %s' % service

        # make sure it's in the service list
        if self.rc.srem('discovery:service_list',
                        self.get_service_key(service=service)):

            # remove it's port
            self.rc.srem(self.get_port_key(service=service),service.port)

            # check if there are any ports for this service on this host
            if self.rc.scard(self.get_port_key(service=service)) == 0:
                # if not remove it's host record
                self.rc.srem(self.get_host_key(service=service),service.name)

            return True

        return False


    def find_service(self, service_name):
        """
        returns matching service obj if found.
        raises exception if not found
        """

        print 'finding service: %s' % service_name

        # check and see if we have a host running this service
        hosts = self.rc.smembers(self.get_host_key(service_name))

        # if we don't have hosts, we have a problem
        if not hosts:
            raise o.NotFound('Could not find a host running given service')

        # pick a host to return to them
        host = random.choice(list(hosts))

        # now find a port for it to run on
        ports = self.rc.smembers(self.get_port_key(service_name,host))

        if not ports:
            raise o.NotFound('Could not find port for given service')

        # choose a port
        port = random.choice(list(ports))

        # and we're ready to return
        s = o.Service(host=host,port=int(port),name=service_name)
        return s


def run():
    # since I know i'm discovery i know to import strait
    # from my lib
    from lib import serve_service
    serve_service(Discovery, DiscoveryHandler(),is_discovery=True)

if __name__ == '__main__':
    run()
