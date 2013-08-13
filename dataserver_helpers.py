import time
DATA_DIRECTORY = r'C:\_Data'

def dataserver_client(serveraddr='127.0.0.1', serverport=55556, localaddr='127.0.0.1'):
    import objectsharer as objsh
    if objsh.helper.backend is None:
        zbe = objsh.ZMQBackend()
        zbe.start_server(addr=localaddr)
        zbe.connect_to('tcp://%s:%d' % (serveraddr, serverport))  # Data server
    return objsh.helper.find_object('dataserver')

def get_file(filename, timestamp_group=False, **kwargs):
    c = dataserver_client()
    f = c.get_file(filename)
    if timestamp_group:
        date = time.strftime('%y-%m-%d')
        if date in f:
            f = f[date]
        else:
            f = f.create_group(date)
        f = f.create_group(time.strftime('%H:%M:%S'))
    return f

def run_dataserver(qt=False):
    from dataserver.dataserver import start
    import os
    os.chdir(DATA_DIRECTORY)
    start(qt)