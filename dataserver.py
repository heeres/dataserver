# Shared python HDF5 data file server
# Reinier Heeres <reinier@heeres.eu>, 2013
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

import os
import logging
logging.getLogger().setLevel(logging.INFO)
import objectsharer as objsh
import time
import h5py
import numpy as np

#NOTE: the emit functions are provided by objectsharer after calling register()

class DataSet(object):
    '''
    Shareable wrapper for HDF5 data sets.
    Use indexing ("[:]") to access the actual data.
    '''

    def __init__(self, h5f, group):
        self._h5f = h5f
        self._group = group
        self._name = h5f.name.split('/')[-1]

        fullname = h5f.file.filename + h5f.name
        dataserv._register(fullname, self)

    def __getitem__(self, idx):
        if self._h5f.shape[0] == 0 and idx == slice(None, None, None):
            return np.array([])
        return self._h5f[idx]

    def __setitem__(self, idx, val):
        self._h5f[idx] = val
        self.flush()
        self.emit_changed()

    def emit_changed(self):
        self._group.emit_changed(self._name)

    def set_attrs(self, **kwargs):
        '''
        Set HDF5 attributes.
        '''
        for k, v in kwargs.iteritems():
            self._h5f.attrs[k] = v
        self.flush()
        self.emit('attrs-changed', kwargs)

    def get_attrs(self):
        '''
        Get HDF5 attributes.
        '''
        return dict(self._h5f.attrs)

    def get_xpts(self):
        x0 = self._h5f.attrs['x0']
        xscale = self._h5f.attrs['xscale']
        npts = len(self._h5f)
        x1 = x0 + xscale * (npts - 1)
        return np.linspace(x0, x1, npts)

    def get_ypts(self):
        y0 = self._h5f.attrs['y0']
        yscale = self._h5f.attrs['yscale']
        npts = self._h5f.shape[1]
        y1 = y0 + yscale * (npts - 1)
        return np.linspace(y0, y1, npts)

    def get_extent(self):
        """
        Return the boundaries of the dataset. (x0, x1) if rank 1. (x0, x1, y0, y1) if rank 2
        """
        x0 = self._h5f.attrs['x0']
        xscale = self._h5f.attrs['xscale']
        x1 = x0 + xscale*(self._h5f.shape[0] - 1)

        if 'y0' in self._h5f.attrs:
            y0 = self._h5f.attrs['y0']
            yscale = self._h5f.attrs['yscale']
            y1 = y0 + yscale*(self._h5f.shape[1]-1)
            return x0, x1, y0, y1

        return x0, x1

    def set_extent(self, x0, x1, y0=None, y1=None):
        """
        Use the current dataset shape to infer the scale parameter given the boundaries
        """
        xscale = (x1 - x0) / (self._h5f.shape[0] - 1)
        self.set_attrs(x0=x0, x1=x1, xscale=xscale)
        if y0 is not None:
            yscale = (y1 - y0) / (self._h5f.shape[1] - 1)
        self.set_attrs(y0=y0, y1=y1, yscale=yscale)

    def extend(self, data):
        data = np.array(data)
        new_shape = list(self._h5f.shape)
        new_shape[0] += data.shape[0]

        if len(new_shape) > 1 and new_shape[1] == 0:
            for i, s in enumerate(data.shape[1:]):
                new_shape[i+1] = s

        assert all(i == j for i, j in zip(new_shape[1:], data.shape[1:])), \
            "incompatible shapes %s, %s" % (self._h5f.shape, data.shape)

        self._h5f.resize(new_shape)

        data = np.array(data)
        self[-data.shape[0]:] = data

    def append(self, data):
        self.extend([data])

    def flush(self):
        self._h5f.file.flush()


class DataGroup(object):
    '''
    Shareable wrapper for HDF5 data group objects.

    Can be indexed to get sub-groups or sets.
    '''

    def __init__(self, h5f):
        self._h5f = h5f
        groupname = h5f.file.filename + h5f.name
        dataserv._register(groupname, self)

    def __getitem__(self, key):
        val = self._h5f[key]

        # See if this object has a proxy already
        fullname = val.file.filename + val.name
        if fullname in dataserv._datagroups:
            return dataserv._datagroups[fullname]

        # Create a proxy
        if isinstance(val, h5py.Group):
            val = DataGroup(val)
        elif isinstance(val, h5py.Dataset):
            val = DataSet(val, self)
        else:
            raise Exception('Unknown HDF5 type: %s' % (val, ))

        return val

    def __setitem__(self, key, val):
        if key in self._h5f:
            if val.shape != self._h5f[key].shape:
                del self._h5f[key]
                self._h5f[key] = val
                fullname = self._h5f.file.filename + self._h5f[key].name
                if fullname in dataserv._datagroups:
                    dataserv._datagroups[fullname]._h5f = self._h5f[key]
            else:
                self._h5f[key][:] = val
        else:
            self._h5f[key] = val
        self.flush()
        self.emit_changed(key)

    def __delitem__(self, key):
        del self._h5f[key]
        self.flush()
        self.emit('removed', key)

    def __contains__(self, item):
        return item in self._h5f

    def emit_changed(self, key=None):
        '''
        Emit changed signal through objectsharer.
        '''
        self.emit('changed', key)

    def create_group(self, key):
        '''
        Create a new sub group.
        '''
        g = self._h5f.create_group(key)
        self.flush()
        self.emit('group-added', key)
        return DataGroup(g)

    def create_dataset(self, name, shape=None, dtype=np.float64, data=None, rank=None, **kwargs):
        '''
        Create a new dataset and return it.
        '''
        maxshape = None
        if rank is not None:
            maxshape = (None,) * rank
            if shape is None:
                shape = (0,) * rank

        ds = self._h5f.create_dataset(name, shape=shape, dtype=dtype, data=data, maxshape=maxshape)
        ds = DataSet(ds, self)
        ds.set_attrs(**kwargs)      # This will flush
        return ds

    def keys(self):
        '''
        Return the available sub-groups and sets.
        '''
        return self._h5f.keys()

    def flush(self):
        self._h5f.file.flush()

    def set_attrs(self, **kwargs):
        for k, v in kwargs.iteritems():
            self._h5f.attrs[k] = v
        self.flush()
        self.emit('attrs-changed', kwargs)

    def get_attrs(self):
        ret = {}
        for k, v in self._h5f.attrs.iteritems():
            ret[k] = v
        return ret

    def close(self):
        dataserv.remove_file(self._h5f.file.filename)



class DataServer(object):
    '''
    Shared data server.

    Can be indexed to get an HDF5 data file object.
    '''

    def __init__(self):
        self._hdf5_files = {}
        self._datagroups = {}

    def _register(self, name, datagroup):
        '''
        Register a new DataGroup object.
        '''
        objsh.register(datagroup)
        self._datagroups[name] = datagroup

    def _unregister(self, name):
        objsh.helper.unregister(self._datagroups.pop(name))

    def __getitem__(self, name):
        return self.get_file(name)

    def get_file(self, fn, open=True):
        '''
        Return a data object for file <fn>.
        If <open> == True (default), open the file in not yet opened.
        '''
        f = self._hdf5_files.get(fn, None)

        if f is None:
            if not open:
                return None
            f = h5py.File(fn, 'a')
            self._hdf5_files[fn] = f
            dg = DataGroup(f)
            self.emit('file-added', fn)
        groupname = f.filename + '/'
        return self._datagroups[groupname]

    def list_files(self, names_only=True):
        files = self._hdf5_files.keys()
        if names_only:
            return files
        else:
            return {f: self._datagroups[f + '/'] for f in files}

    def remove_file(self, fn):
        logging.debug('removing file ' + fn)
        self._hdf5_files.pop(fn).close()
        for name in self._datagroups.keys():
            if name.split('/')[0] == fn:
                del self._datagroups[name]

    def get_data(self, fn, group, create=False):
        '''
        Return a data object for <group> in <file>.
        '''
        fullname = fn + group
        dg = self._datagroups.get(fullname, None)
        return dg

    def quit(self):
        logging.info('Closing files...')
        for file in self._hdf5_files.values():
            if file.id:
                file.close()
        import sys
        sys.exit()

    def hello(self):
        return "hello"

dataserv = DataServer()
objsh.register(dataserv, name='dataserver')

def start(qt=False):
    zbe = objsh.ZMQBackend()
    zbe.start_server(addr='127.0.0.1', port=55556)
    if qt:
        zbe.add_qt_timer(10)
    else:
        import signal
        for sig in (signal.SIGABRT, signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *args: dataserv.quit())
        zbe.main_loop()

if __name__ == "__main__":
    import os
    from dataserver_helpers import DATA_DIRECTORY
    os.chdir(DATA_DIRECTORY)
    start()

