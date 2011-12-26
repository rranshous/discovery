import sys, os.path

# add the thrift code to our path
sys.path.insert(0,
    os.path.abspath(
        os.path.join(
            os.path.abspath(
                os.path.dirname(
                    __file__)),
            './gen-py')))


from discovery import Discovery, ttypes as o
from lib import serve_service, connect, connect_discovery
