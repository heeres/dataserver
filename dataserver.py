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
#logging.getLogger().setLevel(logging.DEBUG)
import objectsharer as objsh
import time
import h5py

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
        return self._h5f[idx]

    def __setitem__(self, idx, val):
        self._h5f[idx] = val
        self._group.emit_changed(self._name)

    def set_attrs(self, **kwargs):
        '''
        Set HDF5 attributes.
        '''
        for k, v in kwargs.iteritems():
            self._h5f.attrs[k] = v

    def get_attrs(self):
        '''
        Get HDF5 attributes.
        '''
        ret = {}
        for k, v in self._h5f.attrs.iteritems():
            ret[k] = v
        return ret

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
        self._h5f[key] = val
        self.emit_changed(key)

    def __delitem__(self, key):
        del self._h5f[key]
        self.emit('removed', key)

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
        return DataGroup(g)

    def create_dataset(self, name, shape=None, dtype=None, data=None, **kwargs):
        '''
        Create a new dataset and return it.
        '''
        ds = self._h5f.create_dataset(name, shape=shape, dtype=dtype, data=data)
        ds = DataSet(ds, self)
        ds.set_attrs(**kwargs)
        return ds

    def keys(self):
        '''
        Return the available sub-groups and sets.
        '''
        return self._h5f.keys()

    def flush(self):
        self._h5f.flush()

    def set_attrs(self, **kwargs):
        for k, v in kwargs.iteritems():
            self._h5f.attrs[k] = v

    def get_attrs(self):
        ret = {}
        for k, v in self._h5f.attrs.iteritems():
            ret[k] = v
        return ret

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

        groupname = f.filename + '/'
        return self._datagroups[groupname]

    def get_data(self, fn, group, create=False):
        '''
        Return a data object for <group> in <file>.
        '''
        fullname = fn + group
        dg = self._datagroups.get(fullname, None)
        return dg

dataserv = DataServer()
objsh.register(dataserv, name='dataserver')

zbe = objsh.ZMQBackend()
zbe.start_server(addr='127.0.0.1', port=55556)
zbe.main_loop()

