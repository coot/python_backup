#!/usr/bin/python
"""
Author: Marcin Szamotulski
"""

"""
Todo:
    send email if the size of the backup exceeds a given value. We can
    implement it using the smptlib module.
Todo:
    the scheduler should checkout on start up if the last backup was done.
    (or at least is should have this as an option), i.e. if the turn has passed
    for makeing a backup.
Todo:
    I want to have a copy of a stamp file in the server (so I can look at
    Dorota backups) these files should be human readable log like file. It
    shall contain: date of the backup, number of files in the backup, its size
    before/after backuped.
"""


import time
import traceback
import sys
import os
import os.path
import atexit
import signal
import locale
from datetime import datetime
try:
    import cpickle as pickle
except ImportError:
    import pickle

from backup import Backup

try:
    from apscheduler.scheduler import Scheduler
    from apscheduler.scheduler import EVENT_JOB_ERROR
except ImportError as err:
    print(err)
    sys.exit(1)
from configobj import ConfigObj
from configobj import UnreprError
from optparse import OptionParser

if not hasattr(os, 'EX_OK'):
    os.EX_OK = 0
if not hasattr(os, 'EX_CONFIG'):
    os.EX_CONFIG = 78

locale.setlocale(locale.LC_TIME, os.getenv("LC_TIME"))

parser = OptionParser()
parser.add_option("-v", "--verbose", dest="verbose",
                  default=False, action="store_true")
parser.add_option("-l", "--log", dest="log_file",
                  default="/var/log/backup_scheduler.log")
parser.add_option("-s", "--stamp_file", dest="scheduler_stamps_file",
                  default="/var/lib/pybackup/backup_scheduler.stamps")
parser.add_option("-d", "--daemon", dest="daemon", default=False,
                  action="store_true", help="detach and run in the background")
(options, args) = parser.parse_args()
parser.destroy()

if options.daemon:
    from backup import createDaemon
    createDaemon()

if not os.path.exists('/var/lib/pybackup'):
    os.makedirs('/var/lib/pybackup')


def log(message):
    if not options.log_file is None:
        try:
            with open(options.log_file, 'a') as log_sock:
                log_sock.write("[%s]: %s\n" % (datetime.now(), message))
        except IOError as e:
            print("line %d: %s" % (sys.exc_info()[2].tb_lineno, e))

# Parse the config file:
config_file = os.path.expanduser("~/.backup.rc")
try:
    config = ConfigObj(config_file, write_empty_values=True, unrepr=True)
except UnreprError as e:
    error_msg = " line %d: %s/.backup.rc: unknown name or type in value." \
                % (e.line_number, os.environ["HOME"])
    sys.stderr.write(error_msg + "\n")
    log(error_msg)
    sys.exit(os.EX_CONFIG)

# Read scheduler_stamps on startup and write them to a file on exit (using
# pickle):
try:
    if (os.path.isfile("%s.tmp" % options.scheduler_stamps_file) and
            not os.path.isfile(options.scheduler_stamps_file)):
        os.rename("%s.tmp" % options.scheduler_stamps_file,
                  options.scheduler_stamps_file)
    elif os.path.isfile("%s.tmp" % options.scheduler_stamps_file):
        os.remove("%s.tmp" % options.scheduler_stamps_file)
    with open(options.scheduler_stamps_file, 'rb') as sock:
        try:
            STAMPS = pickle.load(sock)
        except EOFError as e:
            print("line %d: %s" % (sys.exc_info()[2].tb_lineno, e))
            STAMPS = {}
except IOError as e:
    log("line %d: PyB Warning: %s" % (sys.exc_info()[2].tb_lineno, e))
    STAMPS = {}


@atexit.register
def write_scheduler_stamps():
    """
    Dump the stamps to the scheduler_stamps_file using pickle module.

    The stamp file is written to a temporary file, and then moved
    to optinos.scheduler_stamps_file.
    """
    try:
        try:
            with open("%s.tmp" % options.scheduler_stamps_file, 'wb') as sock:
                pickle.dump(STAMPS, sock)
            os.rename("%s.tmp" % options.scheduler_stamps_file,
                      options.scheduler_stamps_file)
        except NameError as e:
            log(" write_scheduler_stamps NameError: line %d: %s"
                % (sys.exc_info()[2].tb_lineno, e))
    except IOError as e:
        log(" write_scheduler_stamps IOError: line %d: %s"
            % (sys.exc_info()[2].tb_lineno, e))


def cron_STAMP(title):
    """
    This function is run by apscheduler. It updates the STAMP for the title.

    When STAMP[title] is greater than stamp for title run the backup.
    It is scheduled using cron_scheduler option defined in ${HOME}/.backup.rc
    file.
    """
    global STAMPS
    if options.verbose:
        print('[%s] CRON_STAMP' % title)
    STAMPS[title] = time.time()
    write_scheduler_stamps()


def cron_backup(title):
    """
    Make backup, send it to the user@server:directory and update the stamp
    file.

    The backup is made if STAMP[title] is greater than backup.get_stamp()
    """
    global STAMPS
    backup = Backup(title, config[title], search=False, keep=True)
    stamp = backup.get_stamp()
    STAMP = STAMPS.get(title, 0)
    if options.verbose:
        print("state=%s" % backup.state)
        print("[%s] stamp=%f (%s)"
              % (title, stamp,
                 time.strftime("%x %X %Z", time.localtime(stamp))))
        STAMPS_dict = dict(
            map(lambda item: (item[0],
                              time.strftime("%x %X %Z",
                                            time.localtime(item[1]))),
                STAMPS.iteritems()))
        print("[%s] STAMP=%f (%s)" % (title,
                                      STAMP,
                                      STAMPS_dict.get(title, 0)))
        print("STAMP > stamp: %s" % (STAMP > stamp))
    if STAMP > stamp:
        if options.verbose:
            print("[%s] FIND_FILES" % title)
        backup.find_files()
        backup.time = STAMP
        if options.verbose:
            print("[%s] MAKE_BACKUP" % title)
        backup.make_backup()
        backup.log('fsize')
        if options.verbose:
            print("[%s] PUT" % title)
        backup.put()
        msg = "INFO: [%s] backuped to: \"%s\"" % (title, str(backup))
        log(msg)
        if options.verbose:
            print(msg)

sched = Scheduler()


@atexit.register
def shutdown_sched():
    sched.shutdown(wait=True)


def schedule_jobs(config):
    """
    Schedule all the backup jobs
    """
    for title in config:
        try:
            cron_scheduler = config[title]['cron_scheduler']
        except KeyError:
            cron_scheduler = []

        for cron_times in cron_scheduler:
            if str(cron_times[0]) == '':
                sched.add_cron_job(cron_STAMP,
                                   args=[title],
                                   minute=str(cron_times[1]),
                                   coalesce=True)
            elif str(cron_times[1]) == '':
                sched.add_cron_job(cron_STAMP, args=[title],
                                   hour=str(cron_times[0]), coalesce=True)
            else:
                sched.add_cron_job(cron_STAMP, args=[title],
                                   hour=str(cron_times[0]),
                                   minute=str(cron_times[1]), coalesce=True)

        if cron_scheduler != []:
            if options.verbose:
                print("ADDING [%s]" % title)
            sched.add_interval_job(cron_backup,
                                   args=[title],
                                   minutes=1)
schedule_jobs(config)


"""
Signal handlers:
    - to reread the config file on SIGHUP,
    - to do all backups with SIGUSR1.
"""


def reconfigure_sched(signal, frame):
    """
    Reread the config file. And reconfigure the scheduler.
    """
    sched.unschedule_func(cron_backup)
    sched.unschedule_func(cron_STAMP)
    try:
        config = ConfigObj(config_file, write_empty_values=True, unrepr=True)
    except UnreprError as e:
        error_msg = (" line %d: %s/.backup.rc: unknown name or type in value."
                     % (e.line_number, os.environ["HOME"]))
        sys.stderr.write(error_msg + "\n")
        log(error_msg)
        sys.exit(os.EX_CONFIG)
    schedule_jobs(config)
    log("INFO: reconfiguring scheduler")
    sched.print_jobs()
signal.signal(signal.SIGHUP, reconfigure_sched)


def backup_all(signal, frame):
    """
    Backup all.
    """
    log("INFO: backup all")
    for title in config:
        cron_STAMP(title)
signal.signal(signal.SIGUSR1, backup_all)

# def stop(signal, frame):
    # with open("/tmp/stop", "a") as file:
        # file.write("%s at: %s\n" % (signal, time.strftime("%x %X",
                                    # time.localtime(time.time()))))
# signal.signal( signal.SIGCONT, stop )
# signal.signal( signal.SIGHUP, stop )
# signal.signal( signal.SIGBUS, stop )


def listen(event):
    if event.exception:
        if options.verbose:
            print("Listen Error: %s" % event.exception)
            print("".join(traceback.format_tb(event.traceback)))
        log("%s at line %d" % (event.exception, event.traceback.tb_lineno))
        log_msg = ("".join(
            traceback.format_tb(event.traceback)
        )).splitlines(True)
        log_msg = [">>> %s" % line for line in log_msg]
        log("\n%s" % "".join(log_msg))
# I think the following line gives: Error function takes exactly 1 argument (0
# given):
sched.add_listener(listen, EVENT_JOB_ERROR)


def log_STAMPS():
    STAMPS_list = map(lambda item: (item[0],
                                    time.strftime("%x %X %Z",
                                                  time.localtime(item[1]))),
                      STAMPS.iteritems())
    stamps_s = ("STAMPS=%s" % dict(STAMPS_list))
    log(stamps_s)

STAMPS_list = map(lambda item: (item[0],
                                time.strftime("%x %X %Z",
                                              time.localtime(item[1]))),
                  STAMPS.iteritems())
STAMPS_str = str(dict(STAMPS_list))
if options.verbose:
    print("STAMPS=%s" % STAMPS_str)
    log("STAMPS=%s" % STAMPS_str)
    sched.add_interval_job(log_STAMPS, hours=1)

sched.start()
for job in sched.get_jobs():
    print("%s(%s) (trigger %s, next run at %s)"
          % (job.name, job.args, job.trigger, job.next_run_time))

try:
    while True:
        # When used pass instead of time.sleep() function the script uses 100%
        # of cpu.
        time.sleep(60)
except KeyboardInterrupt:
    sys.exit(os.EX_OK)
