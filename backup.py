#!/usr/bin/python
# Author: Marcin Szamotulski

# XXX: compression=None is not working

"""
Todo: when config file contains two the same keywords, configoj raises: configobj.DuplicateError.
Todo: --wake-up (-w): send signal.SIGUSR1 to backup_scheduler.py.
"""

import sys, os, os.path, re, tarfile, glob, subprocess, shutil
import locale
locale.setlocale(locale.LC_TIME, os.getenv("LC_TIME"))
import tempfile
import paramiko, time
import GnuPGInterface
from configobj import ConfigObj, UnreprError, ParseError
from optparse import OptionParser

'''
Arguments
{what[:where]}
        {what}   = name of the section .backup.rc file

        [where]  = destination target (like for scp) (overwrites target value in
                   section {what} of .backup.rc)
'''

# Parse config file
config_file = os.path.expandvars("${HOME}/.backup.rc")
try:
    config = ConfigObj( config_file, write_empty_values=True, unrepr=True )
except UnreprError as e:
    if __name__ == "__main__":
        error_msg = "%s/.backup.rc: unknown name or type in value at line %d.\n" % (os.environ["HOME"], e.line_number)
        sys.stderr.write(error_msg)
        sys.exit(1)
    else:
         raise e
except ParseError as e:
    print("ParseError in %s. %s" % (config_file, e.msg))
    sys.exit(1)

def human_size(size_bytes):
    """
    format a size in bytes into a 'human' file size, e.g. bytes, KB, MB, GB, TB, PB
    Note that bytes/KB will be reported in whole numbers but MB and above will have greater precision
    e.g. 1 byte, 43 bytes, 443 KB, 4.3 MB, 4.43 GB, etc
    """

    if size_bytes == 1:
        return "1 b"

    suffixes_table = [('b',0),('KB',0),('MB',1),('GB',2),('TB',2), ('PB',2)]

    num = float(size_bytes)
    for suffix, precision in suffixes_table:
        if num < 1024.0:
            break
        num /= 1024.0

    if precision == 0:
        formatted_size = "%d" % num
    else:
        formatted_size = str(round(num, ndigits=precision))

    return "%s %s" % (formatted_size, suffix)

def replace_empty(val,pattern,exclude_pattern):
    # Replace matching pattern with pattern if '' or not present. 
    # Replace excludeing pattern with exclude_pattern if '' or nor present.

    try:
        if val[1] == '':
             val[1] = pattern
    except IndexError:
        val.append(pattern)
        return val
    try:
        if val[2] == '':
             val[2] = exclude_pattern
        return val
    except IndexError:
        val.append(exclude_pattern)
        return val

def read_options(options):
    '''
    Read options from the options=config["title"] of ${HOME}/.backup.rc file and
    return a dictionary.
    '''

    try:
        archive_path                = options['archive_path']
    except KeyError:
        print("There is no value for archive in section: \"%s\"")
        sys.exit(1)
    try:
        tg                          = options['target']
    except KeyError:
        tg = ''
        target = ['', '', '']
    if not tg == '':
        match = re.match('(?:([^@]*)@)?(?:([^:]*):)?(.+)', tg)
        target = ['', '', '']
        if match:
            if match.group(1) != None:
                target[0] = match.group(1)
            if match.group(2) != None:
                target[1] = match.group(2)
            if match.group(3) != None:
                target[2] = match.group(3)
    try:
        dirs                        = options['dir']
    except KeyError:
        print("There is no value for dir in section: \"%s\"" % title)
        # XXX: raise an exception which can be cought by the backup_scheduler.py.
        sys.exit(1)
    try:
        compression                 = options['compression']
    except KeyError:
        compression                 = 'bz2'
    try:
        input_files                 = options['input_files']
    except KeyError:
        input_files                 = []

    # 'dir' option entry:
    return_dirs = []
    for dictionary in dirs:
        # expandvars and glob 'dir' values.
        g_dir  = os.path.expandvars(dictionary['dir'])
        for item in [ 'include_pattern', 'include_path_pattern', 'include_files', 'exclude_pattern', 'exclude_dir_pattern', 'exclude_path_pattern', 'exclude_dirs', 'exclude_dirs', 'max_size']:
            if  item != 'include_files' and item != 'exclude_dirs'and item != 'max_size':
                default_value = options.get(item, '')
            elif item == 'exclude_dirs' or item == 'include_files':
                default_value = options.get(item, [])
            else:
                # item == 'max_size'
                default_value = None
                sizeu = dictionary.get('max_size', None)
                match = re.match('(\d+)(\w*)', str(sizeu))
                if match:
                    size, unit = [int(match.group(1)), match.group(2)]
                    if re.match(re.compile('kb', re.I),unit):
                        size = size*1024
                    elif re.match(re.compile('mb', re.I),unit):
                        size = size*1024*1024
                    elif re.match(re.compile('gb', re.I),unit):
                        size = size*1024*1024*1024
                    elif re.match(re.compile('tb', re.I),unit):
                        size = size*1024*1024*1024*1024
                    dictionary['max_size']=size
            dictionary[item]=dictionary.get(item, default_value)
            if item == 'exclude_dirs' and not isinstance(dictionary[item], list):
                print(dictionary[item])
                print("Backup Error: The value of exclude_dirs in dir[%s] in section 'Backup %s' is not a list!" % (dictionary[dir], title))
                sys.exit(2)
        for path in glob.iglob(g_dir):
            if os.path.isdir(path):
                return_dictionary=dictionary.copy()
                return_dictionary['dir']=path
                return_dirs.append(return_dictionary)

    try:
        reciepient              = options['reciepient']
    except KeyError:
        reciepient          = ''
    try:
        passphrase          = options['passphrase']
    except KeyError:
        passphrase          = ''


    return { 'archive_path' : archive_path, \
             'target' : target, \
             'dirs' : return_dirs, \
             'input_files' : input_files, \
             'compression' : compression, \
             'reciepient'  : reciepient, \
             'passphrase'  : passphrase }

class ConnectionError(Exception):
    def __init__(self,progname, return_code, info=""):
        self.progname=progname
        self.return_code=return_code
        self.info=info

    def __str__(self):
        if info == "":
            return "%s returned with error code %d " % (self.progname, self.return_code)
        else:
            return "%s returned with error code %d : %s " % (self.progname, self.return_code, info)


# The main Backup class
class Backup(object):
    """
    The Backup class which makes and sends backups.

    Only the put() method updates the stamp file by default.
    """
    def __init__( self, name, options, search=True, keep=False, gnupg=True):
        """
        self.name           - name of the backup section in ${HOME}/.backup.rc
        self.time           - time stamp recorded in the self.stamp_file
                              it is updated by: self.find_files(), 
        self.stamp_file     - the stamp file to use
        self.path           - path to the archive
        self.file_list      - list of files to archive
        self.size_excluded  - list of excluded files by size
        self.log_file       - log file
        self.log_list       - log list
        self.compression    - "None/bz2/gz/7z" how to compress the tar archive
        self.reciepient     - reciepient to use by GnuPGInterface.GnuPG instance
        self.passphrase     - passphrase to use by GnuPGInterface.GnuPG inctance
        self.keep           - keep the copy of backup on the local drive
        self.encrypted      - internal: True/False
        self.state          - internal: config/list of files/backuped/
        self.tmpdir         - internal: where to get the backup from a remote location
        """

        self.name               = name
        self.option_dict        = read_options(options)
        self.stamp_file         = re.match('linux', sys.platform) and '/var/lib/pybackup/backup.stamps' or os.path.join(os.path.dirname(self.option_dict['archive_path']),'backup.stamps')
        if not os.path.exists('/var/lib/pybackup'):
            os.makedirs('/var/lib/pybackup')
        self._target            = self.option_dict['target']
        self.path               = self.option_dict['archive_path']+".tar"
        self.log_file           = self.option_dict['archive_path']+".log"
        self.compression        = self.option_dict['compression']
        self.keep               = keep
        self.reciepient         = self.option_dict['reciepient']
        self.passphrase         = self.option_dict['passphrase']
        self.encrypted          = False
        self.tmpdir             = None
            # If self.keep is True then the self.put method will not delete the self.path.
        if self.compression != None and self.compression != '':
            self.path += "."+self.compression
        self.time  = time.time()
        if search:
            self.file_list, self.size_excluded = self.__find_files(self.option_dict['dirs'], self.option_dict['input_files'])
            self.state = 'list of files'
        else:
            self.file_list, self.size_excluded = [[],[]]
            self.state = 'config'
        self.log_list           = []
        size = 0
        for file in self.file_list:
            try:
                fsize = os.path.getsize(file)
            except OSError:
                fsize = 0
            size += fsize
            self.log_list += [[file, fsize]]
        self.size = size

    def __str__(self):
        [user, server, directory] = self._target
        if not user is '':
            return "%s@%s:%s" % (user, server, directory)
        else:
            return "%s" % directory

    def __iter__():
        return self.file_list.__iter__()

    def __next__():
        return self.file_list.__next__()

    def target(self,target):
        ''' Set the backup target.

        target is of the format DIR or USER@HOST:DIR (like for scp).'''
        match = re.match('(?:([^@]*)@)?(?:([^:]*):)?(.+)', target)
        self._target = ['', '', '']
        if match:
            if match.group(1) != None:
                self._target[0] = match.group(1)
            if match.group(2) != None:
                self._target[1] = match.group(2)
            if match.group(3) != None:
                self._target[2] = match.group(3)

    def __filter_dirs(self,root,root_dir,dirname,exclude_path_pattern,exclude_dir_pattern,exclude_dirs):
        """
        root          - the root of currently scanneed directory (as returened by os.walk())
        root_dir      - the root directory where we started scaning the files
        dirname       - directory name in the root (as returened by os.walk())
        """

        path        = os.path.join(root, dirname)
        relpath     = os.path.relpath(os.path.join(root, dirname), root_dir)

        if (path in [os.path.normpath(os.path.join(root_dir, directory)) for directory in exclude_dirs]) or \
                exclude_dir_pattern  != '' and re.search(exclude_dir_pattern, dirname) or \
                exclude_path_pattern != '' and re.search(exclude_path_pattern, relpath):
            return False
        else:
            return True

    def __scan_directory(self,directory,include_pattern,include_path_pattern,exclude_pattern,exclude_path_pattern,exclude_dir_pattern,exclude_dirs,max_size):
        # find files which under one of the directory dir, which match the pattern

        file_list=[]
        size_excluded=[] # list of files excluded by size
        for root, dirs, files in os.walk(directory):
            dirs[:]=[item for item in dirs if self.__filter_dirs(root=root,root_dir=directory,dirname=item,exclude_path_pattern=exclude_path_pattern,exclude_dir_pattern=exclude_dir_pattern,exclude_dirs=exclude_dirs)]
            for file in files:
                fpath = os.path.normpath(os.path.join(root,file))
                path = os.path.relpath(fpath, directory)
                try:
                    fsize = os.path.getsize(fpath)
                except OSError:
                    fsize = 0
                if max_size and fsize > max_size:
                    # size check:
                    cond = False
                    size_excluded.append(fpath)
                else:
                    # pattern matching:
                    if  exclude_pattern == '' and exclude_path_pattern == '':
                        if re.search(include_pattern, file):
                            cond = True
                        elif include_path_pattern != '' and re.search(include_path_pattern, path):
                            cond = True
                        else:
                            cond = False
                    elif (exclude_pattern != '' and exclude_path_pattern == '') or (exclude_pattern == '' and exclude_path_pattern != ''):
                        if exclude_pattern != '':
                            pat         = exclude_pattern
                            fname       = file
                        else:
                            pat         = exclude_path_pattern
                            fname       = path
                        if re.search(include_pattern, fname) and not re.search(pat, fname):
                            cond = True
                        elif include_path_pattern != '' and re.search(include_path_pattern, path) and not re.search(pat, fname):
                            cond = True
                        else:
                            cond = False
                    else:
                        # Bothe exclude_pattern and exclude_path_pattern are non empty
                        if re.search(include_pattern, file) and not re.search(exclude_pattern, file) and not re.search(exclude_path_pattern, path):
                            cond = True
                        elif include_path_pattern != '' and re.search(include_path_pattern, path) and not re.search(exclude_pattern, file) and not re.search(exclude_path_pattern, path):
                            cond = True
                        else:
                            cond = False
                if cond:
                    file_list.append(os.path.join(root,file))
        return [file_list,size_excluded]

    def __find_files( self, dirs, input_files ):
        # Make list of files using 'dirs' and 'input_files' (options).

        files= []
        size_excluded = [] # files excluded by size
        print("Searching for files:")
        for entry in dirs:
            directory               = entry['dir']
            include_files           = entry['include_files']
            include_pattern         = entry['include_pattern']
            include_path_pattern    = entry['include_path_pattern']
            exclude_pattern         = entry['exclude_pattern']
            exclude_dir_pattern     = entry['exclude_dir_pattern']
            exclude_path_pattern    = entry['exclude_path_pattern']
            exclude_dirs            = entry['exclude_dirs']
            max_size                = entry['max_size']
            n_files, s_excluded = self.__scan_directory(directory, include_pattern, include_path_pattern, \
                exclude_pattern, exclude_path_pattern, exclude_dir_pattern, exclude_dirs, max_size)
            files.extend(n_files)
            for file in map(lambda file: os.path.join(directory, file), include_files):
                if not file in files:
                    files.append(file)
            size_excluded.extend(s_excluded)
        print("Found %d files." % len(files))

        print("Scanning input files.")
        for input_file in input_files:
            try:
                ifo=open(input_file, 'r')
            except IOError as e:
                print(str(e))
                lines = []
            else:
                lines=ifo.readlines()
                ifo.close()
            lines[:]=filter(lambda line: not re.match('^\s*$|^\s*#',line), lines)
            for line in lines:
                # glob and expandvars in input files:
                files.extend(glob.glob(os.path.expandvars(line)))

        return [files, size_excluded]

    def __make_tarball( self, archive_path, files, stamp=time.time(), compression='7z' ):
        """
        Make tarball from files using compression ('7z', 'gz', 'bz2', '' (None is also valid))
        include archive_stamp file. This should agree with stamp in the stamp
        file.  Returns path to compressed archive.
        """

        print("Making tar ball.")
        if compression == "gz":
            ext = ".tar.gz"
            mode = ":gz"
            output_path  = archive_path+ext
        elif compression == "bz2":
            ext = ".tar.bz2"
            mode = ":bz2"
            output_path  = archive_path+ext
        elif compression == "7z":
            ext = ".tar"
            mode = ""
            output_path  = archive_path+".tar.7z"
        else:
            ext = ".tar"
            mode = ""
            output_path  = archive_path+ext
        try:
            if compression == "7z":
                shutil.move(archive_path+ext+".7z", archive_path+ext+".7z.old")
            else:
                shutil.move(archive_path+ext, archive_path+ext+".old")
        except IOError:
            if os.path.exists(archive_path+ext+".7z"):
                print("Warning: can not make a backup copy of the archive.")
        # posix format of tar files solves unicode problems for tar files.
        try:
            tar_o=tarfile.open(archive_path+ext, 'w'+mode)
        except IOError as e:
            print("line %d: %s" % (sys.exc_info()[2].tb_lineno, e))
        else:
            tar_o.PAX_FORMAT=True
            tar_stamp_path = os.path.join(os.path.dirname(archive_path), 'archive_stamp')
            try:
                tar_stamp = open(tar_stamp_path, 'w')
            except IOError as e:
                print("line %d: %s" % (sys.exc_info()[2].tb_lineno, e))
            else:
                tar_stamp.write('%s\t\t%s\n' % ( stamp, time.strftime('%x %X %Z', time.localtime(stamp)) ) )
                tar_stamp.close()
            tar_o.add(tar_stamp_path, 'archive_stamp')
            os.remove(tar_stamp_path)
            for file in files:
                try:
                    tar_o.add(file)
                except IOError, e:
                    if e.errno == 13:
                        print("line %d: %s" % (sys.exc_info()[2].tb_lineno, e))
                    else:
                        raise
                except OSError, e:
                    if e.errno == 2:
                        print("line %d: %s" % (sys.exc_info()[2].tb_lineno, e))
                    elif e.errno ==13:
                        print("line %d: %s" % (sys.exc_info()[2].tb_lineno, e))
                    else:
                        raise
            tar_o.close()
        if compression == '7z':
            ext = '.7z'
            try:
                cmd = ['7z', 'a', '-mx9', archive_path+'.tar.7z', archive_path+'.tar']
                subprocess.Popen(cmd).wait()
                # subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE).wait()
            except:
                # Delete the incomplete archive and raise any exception:
                # in this way re-running make_backup() fuction will not overwrire the
                # file archive_path+".tar.7z.old".
                try:
                    os.remove(archive_path+'.tar.7z')
                except IOError as e:
                    print("line %d: %s" % (sys.exc_info()[2].tb_lineno, e))
                raise
            try:
                os.remove(archive_path+'.tar')
            except OSError as e:
                print("line %d: %s" % (sys.exc_info()[2].tb_lineno, e))
            if os.path.exists(archive_path+'.tar.7z.old'):
                os.remove(archive_path+'.tar.7z.old')
        else:
            if os.path.exists(archive_path+'.tar.7z.old'):
                os.remove(archive_path+ext+'.old')
        return output_path

    def find_files(self):
        ''' Find files for the backup (public interface).

        calls _find_files method.'''
        self.time = time.time()
        self.file_list, self.size_excluded = self.__find_files(self.option_dict['dirs'], self.option_dict['input_files'])
        return

    def __encrypt(self):
        gnupg = GnuPGInterface.GnuPG()
        if not self.reciepient == '':
            print("Encrypting.")
            gnupg.options.reciepients=[self.reciepient]
            try:
                input_fo  = open(self.path)
                output_fo = open(self.path+'.gpg', 'w')
            except IOError as e:
                print(str(e))
            else:
                gnupg_enc = gnupg.run(['--encrypt'], attach_fhs={'stdin' : input_fo, 'stdout' : output_fo})
                gnupg_enc.wait()
                self.encrypted = True
                input_fo.close()
                output_fo.close()
        elif not self.passphrase == '':
            print("Encrypting.")
            try:
                input_fo  = open(self.path)
                output_fo = open(self.path+'.gpg', 'w')
            except IOError as e:
                print(str(e))
            else:
                gnupg_enc = gnupg.run(['--encrypt'], create_fhs=['passphrase'], attach_fhs={'stdin' : input_fo, 'stdout' : output_fo})
                gnupg_enc.handlers['passphrase'].write(self.passphrase)
                gnupg_enc.handlers['passphrase'].close()
                gnupg_enc.wait()
                self.encrypted = True
                input_fo.close()
                output_fo.close()
        else:
            return
        if self.encrypted:
            if not self.keep:
                # Keep the non encrypted file.
                os.remove(self.path)
            self.path += '.gpg'
            # Always remove gpg file
            self.remove = True

    def __decrypt(self):
        if self.encrypted:
            try:
                input_fo   = open(self.path)
                output_fo  = open(os.path.splitext(self.path)[0], 'w')
            except IOError as e:
                print(str(e))
            else:
                if not self.recipient == '':
                    gnupg.options.reciepients=[self.reciepients]
                    gnupg_enc  = gnupg.run(['--decrypt'], attach_fhs={'stdin' : input_fo, 'stdout' : output_fo})
                elif not self.passphrase == '':
                    input_fo   = open(self.path)
                    output_fo  = open(os.path.splitext(self.path)[0], 'w')
                    gnupg_enc = gnupg.run(['--decrypt'], create_fhs=['passphrase'], attach_fhs={'stdin' : input_fo, 'stdout' : output_fo})
                    gnupg_enc.handlers['passphrase'].write(self.passphrase)
                    gnupg_enc.handlers['passphrase'].close()
                gnupg_enc.wait()
                self.encrypted = False
                input_fo.close()
                output_fo.close()
            if not self.encrypted:
                # Remove gpg file:
                os.remove(self.path)
                self.path = os.path.splitext(self.path)[0]
            else:
                print("Could not decrypt the backup file '%s'" % self.path)
                sys.exit(1)

    def __server_put( self, user, server, local_path, remote_path ):
        # send backup_file (full path) to user@server:/backup_dir

        print("Sending "+str(local_path)+" to "+str(user)+"@"+str(server)+":"+str(remote_path))
        try:
            # Open ssh conection using paramiko module:
            ssh         = paramiko.SSHClient()
            # Load system host keys (authentication with ssh keys)
            ssh.load_system_host_keys()
            ssh.connect(server,username=user)
        except paramiko.BadHostKeyException:
            raise ConnectionError('paramiko SshClient', 'BadHOstKeyException')
        except paramiko.PasswordRequiredException:
            raise ConnectionError('paramiko SshClient', 'PasswordRequiredException', 'pybackup only authenticates using ssh-keys')
        except paramiko.BadAuthenticationType:
            raise ConnectionError('paramiko SshClient', 'AuthenticatioError')
        except paramiko.ssh_exception.PartialAuthentication:
            raise ConnectionError('paramiko SshClient', 'SshException')
        else:
            # sftp connection:
            try:
                sftp        = ssh.open_sftp()
            except paramiko.SFTPError:
                raise ConnectionError('paramiko SFTP', 'SFTPError')
            except paramiko.SSHException:
                raise ConnectionError('paramiko SFTP', 'SFTPError')
            else:
                try:
                    sftp.put(local_path, remote_path)
                except paramiko.SShException as e:
                    raise ConnectionError('paramiko SFTP', 'SshException', info='%s' % e)
            finally:
                sftp.close()
        finally:
            # Close ssh:
            ssh.close()

    def __server_get( self, user, server, remote_path, local_path ):
        # send backup_file (full path) to user@server:/backup_dir

        try:
            # Open ssh conection using paramiko module:
            ssh         = paramiko.SSHClient()
            # Load system host keys (authentication with ssh keys)
            ssh.load_system_host_keys()
            ssh.connect(server,username=user)
        except paramiko.BadHostKeyException:
            raise ConnectionError('paramiko SshClient', 'BadHOstKeyException')
        except paramiko.PasswordRequiredException:
            raise ConnectionError('paramiko SshClient', 'PasswordRequiredException', 'pybackup only authenticates using ssh-keys')
        except paramiko.BadAuthenticationType:
            raise ConnectionError('paramiko SshClient', 'AuthenticatioError')
        except paramiko.ssh_exception.PartialAuthentication:
            raise ConnectionError('paramiko SshClient', 'SshException')
        else:
            # sftp connection:
            try:
                sftp        = ssh.open_sftp()
            except paramiko.SFTPError:
                raise ConnectionError('paramiko SFTP', 'SFTPError')
            except paramiko.SSHException:
                raise ConnectionError('paramiko SFTP', 'SFTPError')
            else:
                sftp.get(remote_path, local_path)
            finally:
                sftp.close()
        finally:
            # Close ssh:
            ssh.close()

    def make_backup(self):
        ''' Make the backup (compress and encrypt).

        backup files from self._file_list list
        '''
        archive_path                = self.option_dict['archive_path']

        self.path = self.__make_tarball( archive_path, self.file_list, self.time, self.compression )
        self.__encrypt()
        self.state                  = 'backuped'

    def delete_backup(self):
        ''' Delete the backup file (self.path).'''
        try:
            os.remove(self.path)
        except IOError as e:
            print(str(e))
        self.state = 'list of files'

    def add_files( self, nfiles ):
        ''' Add files to the existing backup.

        Add files from a list then use Backup.delete() and Backup.make_backup() methods.'''
        self.file_list.append(nfiles)
        self.log_list           = []
        size = 0
        for file in self.file_list:
            try:
                fsize = os.path.getsize(file)
            except OSError:
                fsize = 0
            size += fsize
            self.log_dict.append([file, fsize])
        self.log_dict[size] = size
        if state == 'backuped':
            delete_backup(self)
            make_backup(self)

    def log( self, sort='fsize' ):
        """
        Log files and file sizes, exclded files by size.
        """

        # sort == 'fsize'/'fname'/None
        sorted_log=self.log_list[:]
        sorted_log=sorted(sorted_log, key=lambda i:-i[1])
        s_excluded=self.size_excluded[:]
        def s_map(val):
            try:
                fsize = os.path.getsize(val)
            except OSError:
                fsize = 0
            return [val, fsize]
        s_excluded=map(s_map,s_excluded)
        s_excluded=sorted(s_excluded, key=lambda i:-i[1])
        def join_logline(val):
            if sort == 'fsize':
                return(human_size(val[1])+"\t"+val[0]+"\n")
            else:
                return(val[0]+"\t\t\t"+human_size(val[1])+"\n")
        sorted_log=map(join_logline,sorted_log)
        s_excluded=map(join_logline,s_excluded)

        try:
            log=open(self.log_file, 'w')
        except IOError as e:
            print("line %d: %s" % (sys.exc_info()[2].tb_lineno, e))
        else:
            try:
                tarball_size = human_size(os.path.getsize(self.path))
            except OSError:
                tarball_size = "error"
            log.writelines(['Size of files: '+human_size(self.size)+'\n', 'Size of tarball: '+tarball_size+'\n', 'Number of files: '+str(len(sorted_log))+'\n'])
            log.writelines(['Files excluded by size:\n']+s_excluded+["\n"])
            log.writelines(['Files archived:\n']+sorted_log)
            log.close

    def server_put(self):
        [user, server, directory] = self._target
        try:
            self.__server_put(user,server,self.path,os.path.join(directory,os.path.basename(self.path)))
        except ConnectionError as e:
            # Debug:
             print("%s (%d) : %s" % (e.progname, e.return_code, e.info))
             return

    def update_stamp(self):
        try:
            stamp_fo = open(self.stamp_file, 'r')
            # We read the stamp_file, to filter the stamps.
            stamps = stamp_fo.readlines()
        except IOError as e:
            stamps = []
        else:
            stamp_fo.close()
        """
        Format of the stamp file:
            backup_name         time_stamp      time_stamp_human_readable
        where backup_name = title, and time_stamp = time since epoch.
        """
        def f_stamps(val):
            return re.match("%s\s+(?:\d+\.\d+|None)" % re.escape(self.name), val) == None
        stamps=filter( f_stamps, stamps )
        stamps.append('%s\t\t\t%f\t\t%s\n' % (self.name, self.time, time.strftime('%x %X %Z', time.localtime(self.time))))
        try:
            stamp_fo = open(self.stamp_file, 'w')
        except IOError as e:
            print("line %d: %s" % (sys.exc_info()[2].tb_lineno, e))
        else:
            stamp_fo.writelines(stamps)
            stamp_fo.close()

    def get_stamp(self):
        # Read the stamp file and return the last stamp corresponding to self.name.
        # The file contains time stamps of backups copied using server_put() method.
        try:
            stamp_fo = open(self.stamp_file, 'r')
        except IOError as e:
            stamps=[]
            print("backup.py: line %d: %s" % (sys.exc_info()[2].tb_lineno, e))
        else:
            stamps = stamp_fo.readlines()
            stamp_fo.close()
        def g_time(val):
            match = re.match(re.escape(self.name)+'\s*(\d+\.\d+)',val)
            if match:
                return float(match.group(1))
            else:
                return float(0)
        l = map(g_time, filter( lambda val: ( re.match('%s\s+\d+\.\d+' % re.escape(self.name), val) ), stamps ))
        if len(l):
            return max(l)
        else:
            return 0

    def put(self):
        # Universal method of puting the backup to self._target
        if self._target[0] != '' and self._target[1] != '':
            self.server_put()
        elif self._target[2] != '' and \
                os.path.normpath(self._target[2]) != os.path.normpath(os.path.dirname(self.path)) and \
                os.path.normpath(self._target[2]) != os.path.normpath(self.path):
            shutil.copy(self.path, self._target[2])
        if not self.keep and self._target != ['','',''] and \
                self._target != ['', '', os.path.normpath(os.path.dirname(self.path))]:
            print("Remove: "+self.path)
            os.remove(self.path)
        self.update_stamp()

    def server_get(self):
        """
        Get backup from the server and decrypt.
        """

        self.tmpdir = tempfile.mkdtemp(dir=os.path.dirname(self.path))
        [user, server, directory] = self._target
        remote_path = os.path.join(directory,os.path.basename(self.path))
        local_path = os.path.join(self.tmpdir,os.path.basename(self.path))
        if not self.reciepient == '' or not self.passphrase == '':
            remote_path    += '.gpg'
            local_path     += '.gpg'
            self.path      = local_path
            self.encrypted = True
        self.__server_get(user,server, remote_path, local_path)
        self.__decrypt()

    def unpack( self, remove=False ):
        # decrypt and unpack 7z archive (this is done in the same directory)
        # if remove=True the 7z archive will be removed (but then in next run
        # backup.py will not find the archive?, check this)

        self.__decrypt()
        if os.path.splitext(self.path)[1] == '.7z':
            if os.path.exists(os.path.splitext(self.path)[0]):
                os.remove(os.path.splitext(self.path)[0])
            # '7z' needs to work in the same directory.
            cwd = os.getcwd()
            os.chdir(os.path.dirname(self.path))
            cmd = [ '7z', 'e', self.path ]
            subprocess.call(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            os.chdir(cwd)
            if remove:
                os.remove(self.path)
            [path, ext] = os.path.splitext(self.path)
            if ext == ".bz2":
                self.compression = "bz2"
            elif ext == ".gz":
                self.compression = "gz"
            else:
                self.compression = None
            self.path = os.path.splitext(self.path)[0]

    def find_file( self, pattern, basename=True ):
        # find file matching pattern in self. But copy the backup file into
        # tmpdir and unpack (7z) it there if necessary. 
        # basename = True : match the pattern against basenames not the full path.

        self.unpack()
        if os.path.splitext(self.path) == ".bz2":
            self.compression = "bz2"
            mode = ":bz2"
        elif os.path.splitext(self.path) == ".gz":
            self.compression = "gz"
            mode = ":gz"
        else:
            self.compression = None
            mode = ""
        try:
            tar_o = tarfile.open(path, "r"+mode)
        except IOError as e:
            print(str(e))
        else:
            names = tar_o.getnames()
            def filter_f(val):
                if basename:
                    val = os.path.basename(val)
                if re.search(pattern, val):
                    return True
                else:
                    return False
            names = filter(filter_f, names)
            print("\n".join(names))
            tar_o.close()

    def get_member( self, member, directory='__selfpath__' ):
        # get member {member} of the archive to directory {directory}.
        # return path to file or None
        # member might have leading '/' - it will be removed.
        if directory == '__selfpath__':
            directory = os.path.dirname(self.path)
        elif directory == '':
            directory=os.getcwd()
        if re.match('/', member):
            member = str.strip(member, '/')

        self.unpack(remove=False)
        if self.compression == 'bz2':
            mode = ':bz2'
        elif self.compression == 'gz':
            mode = ':gz'
        else:
            mode = ''
        try:
            tar_o = tarfile.open(self.path, 'r'+mode)
        except IOError as e:
            print(str(e))
        else:
            file_tarinfo=tar_o.getmember(member)
            if file_tarinfo.isfile():
                fo = tar_o.extractfile(member)
                flines = fo.read()
                fo.close()
                fpath = os.path.join(directory,os.path.basename(file_tarinfo.name))
                # The file will be overwritten without warning.
                try:
                    fpath_o = open(fpath, 'w')
                except IOError as e:
                    print(str(e))
                else:
                    fpath_o.write(flines)
                    fpath_o.close()
                return fpath
            else:
                return None
            tar_o.close()

if __name__ == '__main__':
    usage   = "%prog [options] {what[:where]} ..."
    parser  = OptionParser(usage=usage)

    # Note:
    # I need embedded stamp: then I can recognize that an archive is actual one and use it without re-downloading!

    # Note:
    # tar file will contain full path and it is possible to get file using full path.

    # Config file to use:
    parser.add_option("-c", "--config", dest="config_file", default=os.path.join(os.path.expandvars("$HOME"), ".backup.rc"), help="use specified config file")
    # Compression:  (not implemented)
    parser.add_option("--compression", dest="compression", default="7z", help="use one of the compressions: 7z (default), bz2, gz, None")
    # Keep/Delete the archive:  (not implemented)
    parser.add_option("-K", "--nokeep", dest="keep", default=True, action="store_false", help="keep the backup file (usefull if you send it to a remote host)")
    # File pattern to find in backup:
    parser.add_option("-f", "--find_file", dest="fpattern", default="", help="find file in the backup")
    # Get the member using full or relative (to the current directory) path:
    parser.add_option("--get_member", dest="member", default="", help="get a file from the backup")
    # Encrypt (force that backup is not encrypted)
    parser.add_option("-E", "--noencrypt", dest="force_no_encrypt", default=False, action="store_true", help="do not encrypt backup")

    (options, args) = parser.parse_args()
    if len(args) >= 2:
        target = args[1]
    else:
        target = None
    config_file=options.config_file
    fpattern = options.fpattern
    member = options.member
    try:
        name = args[0]
    except IndexError:
        print(usage)
        print("At least one argumet is needed,")
        print("{what}[:where] is a list of section names of the config file ${HOME}/.backup.rc,")
        print("[:where] can be a directory name (with escaped spaces )or a remote location: user@server:file_path.")
        print("         It will overwrite the target variable from the config file.")
        sys.exit(1)
    if fpattern != "":
        backup = Backup( name, config[name], search=False, keep=options.keep )
        if not target == None:
            backup.target(target)
        # We should check if we need to get a backup from server or use the
        # one that is at archive_path. For this we can use archive_path
        # included in the archive. For this it might be better if the stamp
        # was in a seprate file, downloading it is faster than downloading
        # whole archive. Another solution is to unpack it on the server but
        # this requires 7z.
        backup.find_file(fpattern)
        sys.exit(0)
    if member != "":
        backup = Backup( name, config[name], search=False, keep=options.keep )
        if not target == None:
            backup.target(target)
        # Get the member using full or relative (to the current directory) path:
        mpath=backup.get_member(os.path.normpath(os.path.join(os.getcwd(),member)))
        print(mpath)
        sys.exit(0)
    for arg in args:
        print(arg)
        option_match = re.match('([^:]*)(?::(.*$))?',arg)
        name = option_match.group(1)
        tg = option_match.group(2)
        backup = Backup( name, config[name], search=True, keep=options.keep )
        if options.force_no_encrypt:
            backup.reciepient = ''
            backup.passphrase = ''
        if not tg == None:
            backup.target(tg)
        backup.log('fsize')
        backup.make_backup()
        backup.put()
