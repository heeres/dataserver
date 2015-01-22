# stresstest.py, simple script to run as a dataserver stress test

import numpy as np
import objectsharer as objsh
objsh.helper.backend.start_server('127.0.0.1')
objsh.helper.backend.connect_to('tcp://127.0.0.1:55556')
datasrv = objsh.find_object('dataserver')

NWRITES = 1000
NARS = 20
ARLEN = 100
NFILES = 4
FNS = [('test%d.h5'%i) for i in range(NFILES)]
RELEASE = False

for i in range(NWRITES):
    if (i % 10) == 0:
        print 'Rec%d' % (i,)
    fn = FNS[np.random.randint(NFILES)]
    f = datasrv.get_file(fn)
    oname = 'rec%d'%i
    ar = np.random.rand(ARLEN)
    if oname in f:
        print 'Using existing %s from %s'%(oname, fn)
        g = f[oname]
    else:
        print 'Creating %s in %s'%(oname, fn)
        g = f.create_dataset(oname, data=ar)
    g[:] = ar
    if np.count_nonzero(g[:] != ar) != 0:
        raise Exception('Array does not match')
    if RELEASE:
        g.release()

print 'I am %s' % (objsh.root._OS_UID,)
objsh.helper.backend.add_qt_timer()

