DATA_DIRECTORY = r'C:\_Data'

def dataserver_client(serveraddr='127.0.0.1', serverport=55556, localaddr='127.0.0.1'):
    import objectsharer as objsh
    if objsh.helper.backend is None:
        zbe = objsh.ZMQBackend()
        zbe.start_server(addr=localaddr)
        zbe.connect_to('tcp://%s:%d' % (serveraddr, serverport))  # Data server
    return objsh.helper.find_object('dataserver')

def get_file(filename, **kwargs):
    c = dataserver_client()
    return c.get_file(filename)

def run_dataserver(qt=False):
    import dataserver
    import os
    os.chdir(DATA_DIRECTORY)
    dataserver.start(qt)