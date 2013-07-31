import unittest
import multiprocessing
import dataserver_helpers as ds
import H5Plot
import numpy as np
import h5py
import os
import glob
import time

class ClientServerTestCase(unittest.TestCase):
    def setUp(self):
        self.server_process = multiprocessing.Process(target=ds.run_dataserver)
        self.server_process.start()
        for f in glob.glob(os.path.join(ds.DATA_DIRECTORY, 'test_*.h5')):
            print 'Deleting', f
            os.remove(f)
        time.sleep(.5)

    def tearDown(self):
        if self.server_process.is_alive():
            self.server_process.terminate()

    def testDataPersistence(self):
        c1 = ds.dataserver_client()
        filename = 'test_data_persistence.h5'
        f1 = c1.get_file(filename)
        data = np.random.normal(size=10)
        f1['data'] = data
        c2 = ds.dataserver_client()
        f2 = c2.get_file(filename)
        for name, f in [('primary', f1), ('secondary', f2)]:
            assert all(data == f['data'][:]), 'data failed to match from %s file' % name
        f1.close()
        f3 = h5py.File(os.path.join(ds.DATA_DIRECTORY, filename), 'r')
        assert all(data == f3['data'][:]), 'data failed to match from %s h5py file'

if __name__ == "__main__":
    unittest.main()
