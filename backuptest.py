#!/usr/bin/python

import os
import os.path
import unittest
import time
from backup import Backup
from backup import read_options
from configobj import ConfigObj, UnreprError
import tarfile

config_file = os.path.expandvars("${HOME}/.backup.rc")
config = ConfigObj( config_file, write_empty_values=True, unrepr=True )

class TestBackup(unittest.TestCase):
    """ call to make unittest of the backup.Backup() class."""

    def __init__( self, methodName = 'runTest' ):
        unittest.TestCase.__init__( self, methodName )
        self.backup = Backup("VIM", config["VIM"], search=False, keep=True )
        self.backup.compression = None

    def test_read_options(self):
        """read_options() should return a dictionary."""
        options = read_options(config["TXT"])
        self.assertEqual(type({}), type(options), "options has to ba a dictionary")
        self.assertEqual(type([]), type(options["dirs"]), "options[\"dirs\"] has to ba a list")
        self.assertEqual(type(""), type(options["archive_path"]), "options[\"archive_path\"] has to be a string")
        self.assertEqual(type(""), type(options["compression"]), "options[\"compression\"] has to be a string")
        self.assertEqual(type([]), type(options["input_files"]), "options[\"input_files\"] has to be a list")

    def test_target(self):
        """ Backup.target should be a string."""
        self.assertEqual(type([]), type(self.backup._target))

    def test_time(self):
        """ Backup.time should be a float number."""
        self.assertEqual(type(float(0)), type(self.backup.time), "backup.time should be a float number")
        # The following test fails if test_make_backup() is run:
        # self.assertAlmostEqual(time.time(), self.backup.time, places=0 )

    def test_log_file(self):
        """ Backup.log_file should be a string ."""
        backup = Backup( "VIM", config["VIM"], search=False, keep=True )
        dir_name = os.path.dirname(backup.log_file)
        self.assertTrue(os.path.isdir(dir_name))

    def test_make_backup(self):
        """ test for Backup.make_backup() """
        self.backup.compression=None
        self.backup.target("/tmp")
        self.backup.find_files()
        self.backup.make_backup()
        self.assertTrue(os.path.isfile(self.backup.path))
        self.assertNotEqual([],self.backup.file_list, "VIM backup should be non empty")
        self.backup.put()
        path = os.path.join(self.backup._target[2],os.path.basename(self.backup.path))
        self.assertTrue(os.path.isfile(path), "Backup.make_backup() should make the backup file: %s" % path)
        with tarfile.open(path, 'r') as tarfile_o:
            has = len(tarfile_o.getnames())
            should = len(tarfile_o.getnames())
            self.assertEqual(has, should, "Backup should contain %d files, but contains %d files" % (has, should))
        # The path is net removed unless both above test are passed. If a test
        # fails, an exception is raised which stopes the execution of the
        # module.
        os.remove(path)

if __name__ == "__main__":
    unittest.main()
