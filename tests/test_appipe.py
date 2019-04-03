#
# This file is part of ap_pipe.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import os
import unittest
from unittest.mock import patch

import lsst.utils.tests
import lsst.pex.exceptions as pexExcept
import lsst.daf.persistence as dafPersist

from lsst.ap.pipe import ApPipeTask


class PipelineTestSuite(lsst.utils.tests.TestCase):
    '''
    A set of tests for the functions in ap_pipe.

    TODO: write tests for DM-13567.
    '''

    @classmethod
    def setUpClass(cls):
        try:
            cls.datadir = lsst.utils.getPackageDir("ap_pipe_testdata")
        except pexExcept.NotFoundError:
            raise unittest.SkipTest("ap_pipe_testdata not set up")

        cls.config = ApPipeTask.ConfigClass()
        cls.config.load(os.path.join(cls.datadir, "config", "apPipe.py"))
        cls.config.ppdb.db_url = "sqlite://"
        cls.config.ppdb.isolation_level = "READ_UNCOMMITTED"

    def setUp(self):
        self.butler = dafPersist.Butler(inputs={'root': self.datadir})
        self.dataId = {"visit": 413635, "ccdnum": 42}

    def testGenericRun(self):
        # test the normal workflow of each ap_pipe step
        task = ApPipeTask(self.butler, config=self.config)
        inputRef = self.butler.dataRef("raw", **self.dataId)
        with patch.object(task, "ccdProcessor") as mockCcdProcessor:
            with patch.object(task, "differencer") as mockDifferencer:
                with patch.object(task, "associator") as mockAssociator:
                    with patch.object(task, "diaForcedSource") as mockForcedSource:
                        task.runDataRef(inputRef)
                        mockCcdProcessor.runDataRef.assert_called_once()
                        mockDifferencer.runDataRef.assert_called_once()
                        mockAssociator.run.assert_called_once()
                        mockForcedSource.run.assert_called_once()

    # test reuse keyword to ApPipeTask.runDataRef
    # test that fields in Struct are None if we skip that subtask, and non-None otherwise
    # test input to each subtask (without implementation-dependent assumptions)
    # test calexp template code path
    # failures:
    #    - any subtask failing should cause the pipeline to fail
    #    - Association fails with no database (either OperationalError or ProgrammingError)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
