DATA_DIRECTORY = r'C:\_Data'

def dataserver_client(serveraddr='127.0.0.1', serverport=55556, localaddr='127.0.0.1'):
    import objectsharer as objsh
    if objsh.helper.backend is None:
        zbe = objsh.ZMQBackend()
        zbe.start_server(addr=localaddr)
        zbe.connect_to('tcp://%s:%d' % (serveraddr, serverport))     # Data server
    #zbe.refresh_connection('tcp://%s:%d' % (serveraddr, serverport))     # Data server
    return objsh.helper.find_object('dataserver')

def run_dataserver():
    import dataserver
    import os
    os.chdir(DATA_DIRECTORY)
    dataserver.start()