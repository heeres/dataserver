import time
import objectsharer

DATA_DIRECTORY = r'C:\_Data'

def dataserver_client(serveraddr='127.0.0.1', serverport=55556, localaddr='127.0.0.1'):
    import objectsharer as objsh
    if objsh.helper.backend is None:
        zbe = objsh.ZMQBackend()
        zbe.start_server(addr=localaddr)
        zbe.connect_to('tcp://%s:%d' % (serveraddr, serverport))  # Data server
    return objsh.helper.find_object('dataserver')

def get_file(filename, groupname="", timestamp_group=False, **kwargs):
    if not (filename.endswith('.h5') or filename.endswith('.hdf5')):
        filename += '.h5'
    c = dataserver_client()
    f = c.get_file(filename)
    if groupname:
        groupname = ":" + groupname
    if timestamp_group:
        date = time.strftime('%y-%m-%d')
        if date in f:
            f = f[date]
        else:
            f = f.create_group(date)
        f = f.create_group(time.strftime('%H:%M:%S' + groupname))
    return f

def run_dataserver(qt=False):
    from dataserver import start
    import os
    try:
        os.chdir(DATA_DIRECTORY)
    except:
        pass
    start(qt)


def resolve_file(filename, path):
    file = get_file(filename)
    for group in path.split('/'):
        file = file.get_group(group)
    return file.get_numbered_child()

def get_group(file, groupname):
    if groupname not in file:
        file.create_group(groupname)
    return file[groupname]


def resolve_path(file, path):
    if isinstance(path, str):
        path = path.split("/")
    cur = file
    for p in path:
        cur = get_group(cur, p)
    return cur

def get_numbered_child(file):
    max_n = 0
    for k in file.keys():
        try:
            n = int(k)
            max_n = max(n, max_n)
        except ValueError:
            pass
    return file.create_group(str(max_n+1))

def set_scale(group, xname, yname, dim=0, label=None):
    if isinstance(group, objectsharer.ObjectProxy):
        group.set_scale(xname, yname, dim=dim, label=label)
    else:
        if label is None:
            label = xname
        group[yname].dims.create_scale(group[xname], label)
        group[yname].dims[dim].attach_scale(group[xname])

def update_attrs(group, dict):
    if isinstance(group, objectsharer.ObjectProxy):
        group.set_attrs(**dict)
    else:
        for k in dict:
            print k, dict[k]
            group.attrs[k] = dict[k]
