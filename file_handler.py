#!/usr/bin/python3
# -*- coding: utf-8 -*-

import re
import shutil
import sys

from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import traceback
import argparse
import os
import pwd
import grp
import datetime
import psutil
import io
import subprocess
import smtplib
import uuid

# from email.MIMEMultipart import MIMEMultipart
# from email.MIMEText import MIMEText

print('before :', sys.path)
sys.path.append('/home/fi11222/.local/lib/python3.6/site-packages')
print('after :', sys.path)

import psycopg2

__author__ = 'Nicolas Reimen'

# psycopg2 installation
# pip install psycopg2

# ---------------------------------------------------- Globals ---------------------------------------------------------

g_verbose = False
g_silent = False
g_dry_run = False
g_show_delete = False

g_deleted_files = []

g_dbServer = 'localhost'
g_dbDatabase = 'File_Base'
g_dbUser = 'root'
g_dbPassword = 'murugan!'

g_timeZone = 'Asia/Calcutta'

g_mailSender = 'nr.systems.notifications@gmail.com'
# g_mailSenderPassword = 'NotifNicAcc!'
g_mailSenderPassword = 'ycrgxdhzheqqcexs'

g_mailRecipients = ['nicolas.reimen@gmail.com']

g_smtpServer = 'smtp.gmail.com'
g_gmailSmtp = True

g_script_path = os.path.dirname(os.path.realpath(__file__))
g_err_log_path = os.path.join(g_script_path, 'err_log.txt')
g_err_log_file = None
g_err_presence = False

#: If True, use Amazon AWS specific method to connect to the smtp server (SES)
g_amazonSmtp = False
#: Amazon SES ID (not used)
g_sesIamUser = ''
#: Amazon SES user
g_sesUserName = ''
#: Amazon SES pwd
g_sesPassword = ''

# DROP TABLE if exists public."TB_FILE";
#
# CREATE TABLE public."TB_FILE"
# (
#   "TX_FILE_NAME" text NOT NULL,
#   "TX_FILE_PATH" text NOT NULL,
#   "N_LENGTH" bigint,
#   "DT_LAST_MOD" timestamp without time zone,
#   "S_GROUP" character varying(20),
#   "S_OWNER" character varying(20),
#   "S_PERMISSIONS" character varying(3),
#   "S_EXTENSION" character varying(10),
#   CONSTRAINT "TB_FILE_pkey" PRIMARY KEY ("TX_FILE_NAME", "TX_FILE_PATH")
# )
# WITH (
#   OIDS=FALSE
# );
# ALTER TABLE public."TB_FILE"
#   OWNER TO postgres;

# DROP TABLE if exists public."TB_ACTION";
#
# CREATE TABLE public."TB_ACTION"
# (
#   "ID_ACTION" bigserial,
#   "S_ACTION_TYPE" character varying(1) NOT NULL,
#   "TX_PATH1" text,
#   "TX_PATH2" text,
#   "ID_CYCLE" uuid NOT NULL,
#   CONSTRAINT "TB_ACTION_pkey" PRIMARY KEY ("ID_ACTION")
# )
# WITH (
#   OIDS=FALSE
# );
# ALTER TABLE public."TB_ACTION"
#   OWNER TO postgres;

# DROP TABLE if exists public."TB_CYCLE";
#
# CREATE TABLE public."TB_CYCLE"
# (
#   "ID_CYCLE" uuid NOT NULL,
#   "DT_START" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
#   "DT_END" timestamp without time zone,
#   CONSTRAINT "TB_CYCLE_pkey" PRIMARY KEY ("ID_CYCLE")
# )
# WITH (
#   OIDS=FALSE
# );
# ALTER TABLE public."TB_CYCLE"
#   OWNER TO postgres;

# ---------------------------------------------------- Classes ---------------------------------------------------------


class FileHandler:
    """
    Performs all backup operation on files
    """

    # path prefix (and root) of the current scan
    cm_prefix = None

    # Connection to PostgresQL DB
    cm_db_connection = None

    # RAM storage for the file objects resulting from the source tree scan
    cm_mem_store = dict()

    # RAM buffers for writing to TB_FILE & TB_ACTION respectively
    cm_db_buffer = None
    cm_db_buffer_action = None

    # number of lines written to the TB_FILE buffer (to decide when to purge)
    cm_db_buffer_lines = 0

    # Alternate RAM storage for file objects during dry run
    cm_mem_tmp = dict()

    # dry run flag
    cm_dry_run = True

    # UUID of the current cycle
    cm_cycle_uuid = None

    # statistics counters
    cm_deleted_count = 0
    cm_deleted_size = 0
    cm_copied_count = 0
    cm_copied_size = 0
    cm_total_size = 0

    @classmethod
    def reset(cls):
        """
        Sets the FileHandler back to a pristine state (called before a backup cycle)

        :return: Nothing
        """
        cls.cm_prefix = None
        cls.cm_db_connection = None
        cls.cm_mem_store = dict()
        cls.cm_db_buffer = None
        cls.cm_db_buffer_lines = 0
        cls.cm_mem_tmp = dict()
        cls.cm_dry_run = True
        cls.cm_cycle_uuid = None

        cls.cm_deleted_count = 0
        cls.cm_deleted_size = 0
        cls.cm_copied_count = 0
        cls.cm_copied_size = 0
        cls.cm_total_size = 0

    @classmethod
    def get_count(cls):
        """
        total count of files (to be called after the first scan completes)

        :return: The count
        """
        return len(cls.cm_mem_store)

    @classmethod
    def set_prefix(cls, p_prefix):
        """
        Sets the root/path prefix before a scan starts

        :param p_prefix: The path
        :return: Nothing
        """
        cls.cm_prefix = p_prefix

    @classmethod
    def open_connection(cls, p_test=False):
        """
        Open PostgreSQL connection & sets up buffers

        :param p_test: True --> Perform DB connection check and retrieves table list
        :return:
        """
        cls.cm_db_buffer = io.StringIO()
        cls.cm_db_buffer_action = io.StringIO()
        cls.cm_db_buffer_lines = 0

        cls.cm_db_connection = psycopg2.connect(
            host=g_dbServer,
            database=g_dbDatabase,
            user=g_dbUser,
            password=g_dbPassword
        )

        if not g_silent:
            print('Connection to {0} / {1} established'.format(g_dbServer, g_dbDatabase))

        if p_test:
            # connects to the DB and retrieves list of tables (should be 3)
            l_cursor_read = cls.cm_db_connection.cursor()
            l_cursor_read.execute(
                """SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
            for l_table, in l_cursor_read.fetchall():
                print(l_table)
            l_cursor_read.close()

    @classmethod
    def purge(cls):
        """
        Purge buffers to TB_FILES & TB_ACTION
        :return: Nothing
        """
        if not g_silent:
            print('Purge')

        # buffer to TB_FILE
        cls.cm_db_buffer.seek(0)
        l_cursor_write = FileHandler.cm_db_connection.cursor()

        try:
            l_cursor_write.copy_from(
                cls.cm_db_buffer,
                'TB_FILE',
                columns=(
                    'TX_FILE_NAME',
                    'TX_FILE_PATH',
                    'N_LENGTH',
                    'DT_LAST_MOD',
                    'S_GROUP',
                    'S_OWNER',
                    'S_PERMISSIONS',
                    'S_EXTENSION'
                )
            )
            FileHandler.cm_db_connection.commit()
        except Exception as e:
            FileHandler.cm_db_connection.rollback()

            with open('cm_db_buffer.txt', 'w') as l_fdump:
                cls.cm_db_buffer.seek(0)
                shutil.copyfileobj(cls.cm_db_buffer, l_fdump)

            print('DB ERROR:', repr(e))
            print('cm_db_buffer dumped to cm_db_buffer.txt')

            sys.exit(0)
        finally:
            # release DB objects once finished
            l_cursor_write.close()

        # buffer reset
        cls.cm_db_buffer = io.StringIO()
        cls.cm_db_buffer_lines = 0

        # buffer to TB_ACTION
        if cls.cm_db_buffer_action.tell() > 0:
            cls.cm_db_buffer_action.seek(0)
            l_cursor_write = FileHandler.cm_db_connection.cursor()

            try:
                l_cursor_write.copy_from(
                    cls.cm_db_buffer_action,
                    'TB_ACTION',
                    columns=(
                        'ID_CYCLE',
                        'S_ACTION_TYPE',
                        'TX_PATH1',
                        'TX_PATH2'
                    )
                )
                FileHandler.cm_db_connection.commit()
            except Exception as e:
                FileHandler.cm_db_connection.rollback()

                with open('cm_db_buffer_action.txt', 'w') as l_fdump:
                    cls.cm_db_buffer_action.seek(0)
                    shutil.copyfileobj(cls.cm_db_buffer_action, l_fdump)

                print('DB ERROR:', repr(e))
                print('cm_db_buffer_action dumped to cm_db_buffer_action.txt')

                sys.exit(0)
            finally:
                # release DB objects once finished
                l_cursor_write.close()

            # buffer reset
            cls.cm_db_buffer_action = io.StringIO()

    @classmethod
    def clear_base(cls):
        """
        Clears TB_FILE
        :return: nothing
        """

        if g_verbose:
            print('clear_base()')

        l_cursor_write = FileHandler.cm_db_connection.cursor()

        try:
            l_cursor_write.execute("""
                delete from "TB_FILE"
            """)

            FileHandler.cm_db_connection.commit()
        except Exception as e:
            FileHandler.cm_db_connection.rollback()

            print('DB ERROR:', repr(e))
            print(l_cursor_write.query)

            sys.exit(0)
        finally:
            # release DB objects once finished
            l_cursor_write.close()

    @classmethod
    def close_connection(cls):
        """
        Perform final buffer purge and close PostgreSQL connection
        :return: Nothing
        """
        cls.cm_db_connection.close()
        if not g_silent:
            print('Connection to {0} / {1} closed'.format(g_dbServer, g_dbDatabase))

    @classmethod
    def copy_remaining(cls):
        """
        Copy files remaining in memory from source to destination. These files are those which the source file
        scan created but which the destination scan did not remove, i.e. the files which are present in the source
        but not the destination

        :return: Nothing
        """
        for _, l_file in FileHandler.cm_mem_store.items():
            l_file.copy()

            # !!! During dry run, copy file to the tmp buffer to avoid losing them
            if FileHandler.cm_dry_run:
                FileHandler.cm_mem_tmp[l_file.get_key()] = l_file

    @classmethod
    def execute_command(cls, p_cmd, p_shell=False):
        """
        Execute a command (passed as a list) and recovers the outputs. If stderr or stdout is not empty -->
        log the error

        :param p_cmd: the command (as a string if p_shell or a list otherwise)
        :param p_shell: Popen() shell= parameter
        :return: False if something went wrong, True otherwise
        """
        # access to global variables
        global g_err_presence, g_err_log_file

        l_cmd = p_cmd if not p_shell else ' '.join(p_cmd)
        try:
            l_out = subprocess.Popen(p_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=p_shell)

            l_stdout, l_stderr = l_out.communicate(timeout=180)
            l_stdout = l_stdout.decode('utf-8').strip() if l_stdout is not None else ''
            l_stderr = l_stderr.decode('utf-8').strip() if l_stderr is not None else ''

            if l_out.returncode != 0 or len(l_stderr) > 0:
                g_err_presence = True
                g_err_log_file.write('Command [{0}] ret code: {1}\n{2}{3}----------\n'.format(
                    l_cmd, l_out.returncode,
                    'stdout:' + l_stdout + '\n' if len(l_stdout) > 0 else '',
                    'stderr:' + l_stderr + '\n' if len(l_stderr) > 0 else ''
                ))
                return False
            else:
                return True
        except Exception as e:
            g_err_presence = True
            g_err_log_file.write('Command did not return: {0}:\nErr: {1}\nTraceback:\n{2}----------\n'.format(
                l_cmd, repr(e), traceback.format_exc()
            ))
            return False

    def __init__(self, p_path):
        """
        Initialization of a file object from its (absolute) path.

        :param p_path: file path
        """
        global g_verbose

        l_info = os.stat(p_path)

        # absolute path
        self.m_full_path = p_path
        # just the name of the file
        self.m_filename = os.path.basename(p_path)
        self.m_path_suffix = re.sub('^' + FileHandler.cm_prefix + '/', '', p_path)

        # g_verbose = re.search(r'KSU Email', p_path) is not None

        l_list_path = self.m_path_suffix.split('/')
        l_list_path = l_list_path[:len(l_list_path)-1]
        # the full path minus the prefix and also minus the filename
        self.m_path_suffix = '/'.join(l_list_path)

        if g_verbose:
            print('      m_filename   : ', self.m_filename)
            print('      m_path_suffix: ', self.m_path_suffix)

        # file attributes
        self.m_owner = pwd.getpwuid(l_info.st_uid)[0]
        self.m_group = grp.getgrgid(l_info.st_gid)[0]
        self.m_size = l_info.st_size
        self.m_mode = l_info.st_mode
        self.m_lmd = datetime.datetime.fromtimestamp(l_info.st_mtime)

        # file extension (max 25 char long)
        l_match = re.search('\.(.*)$', self.m_filename)
        if l_match:
            self.m_extension = l_match.group(1).lower()
            if len(self.m_extension) > 25:
                # l_match = re.search('\.([^\.]*)$', self.m_filename)
                l_match = re.search('\.([^.]*)$', self.m_filename)
                if l_match:
                    self.m_extension = l_match.group(1).lower()

                if len(self.m_extension) > 25:
                    self.m_extension = self.m_extension[:11] + '...' + self.m_extension[-11:]
        else:
            self.m_extension = ''

        if g_verbose:
            print('      [User] ', self.m_owner)
            print('      [Group]', self.m_group)
            print('      [LMD]  ', self.m_lmd.strftime('%Y-%m-%d %H:%M:%S'))
            print('      [Perm] ', oct(self.m_mode))
            print('      [Size] ', self.m_size)

    def store_mem(self):
        """
        Store a new file into RAM located dictionary (used during the source scan)
        :return: Nothing
        """
        FileHandler.cm_mem_store[self.get_key()] = self
        FileHandler.cm_total_size += self.m_size

        # no backslash allowed in the filename or path --> must be escaped
        FileHandler.cm_db_buffer.write('{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\n'.format(
            self.m_filename.replace('\\', r'\\'),
            self.m_path_suffix.replace('\\', r'\\'),
            self.m_size,
            self.m_lmd.strftime('%Y-%m-%d %H:%M:%S'),
            self.m_group,
            self.m_owner,
            oct(self.m_mode)[-3:],
            self.m_extension[-10:]
        ))
        FileHandler.cm_db_buffer_lines += 1

        if FileHandler.cm_db_buffer_lines >= 100000:
            FileHandler.purge()

    def get_key(self):
        """
        Key for dictionary storage / lookup: Suffix + file name

        :return: Nothing
        """
        return os.path.join(self.m_path_suffix, self.m_filename)

    def delete(self):
        """
        Deletes a file in the destination tree, presumably because it is no longer present in the source. If
        during dry run, only counts. Logs the action as 'D' if successful, 'Y' otherwise.

        :return: Nothing
        """
        if (g_verbose or g_show_delete) and not g_silent:
            print('Deleting : ' + self.m_full_path)

        if g_show_delete:
            g_deleted_files.append(self.m_full_path)

        FileHandler.cm_deleted_count += 1
        FileHandler.cm_deleted_size += self.m_size

        if not FileHandler.cm_dry_run:
            # subprocess.call(['sudo', 'rm', '-f', self.m_full_path])
            if FileHandler.execute_command(['sudo', 'rm', '-f', self.m_full_path]):
                FileHandler.log_action('D', self.m_full_path, None)
            else:
                FileHandler.log_action('Y', self.m_full_path, None)

    def copy(self):
        """
        Copies a file from source to destination. If during dry run, only counts.  Logs the action as 'C'
        if successful, 'Z' otherwise.

        :return: Nothing
        """
        # constructs target path from current prefix (because we are now during the second scan (i.e. target tree)
        l_target_path = os.path.join(FileHandler.cm_prefix, self.m_path_suffix)
        l_target_file = os.path.join(l_target_path, self.m_filename)
        if g_verbose:
            print('Copying : ', self.m_full_path, 'to', l_target_file)

        FileHandler.cm_copied_count += 1
        FileHandler.cm_copied_size += self.m_size

        if not FileHandler.cm_dry_run:
            # constructs directories if missing
            if not os.path.exists(l_target_path):
                if g_verbose:
                    print('Creating path : ', l_target_path)

                FileHandler.execute_command(['sudo', 'mkdir', '-p', l_target_path])
                FileHandler.execute_command(['sudo', 'chown', 'fi11222:fi11222', l_target_path])

            # cp -p ---> preserve attributes (dates, owner, etc)
            if FileHandler.execute_command(['sudo', 'cp', '-p', self.m_full_path, l_target_file]):
                FileHandler.log_action('C', self.m_full_path, l_target_file)
            else:
                FileHandler.log_action('Z', self.m_full_path, l_target_file)

    def compare(self):
        """
        Compare destination file to source and decide to copy or delete.

        :return: Nothing
        """
        l_original = FileHandler.cm_mem_store.pop(self.get_key(), None)
        if l_original is None:
            self.delete()
            return

        if g_verbose:
            print('      Comparing with :', l_original.m_full_path)

        if FileHandler.cm_dry_run:
            FileHandler.cm_mem_tmp[l_original.get_key()] = l_original

        if self.m_size != l_original.m_size or \
                self.m_lmd != l_original.m_lmd or \
                self.m_mode != l_original.m_mode or \
                self.m_owner != l_original.m_owner or \
                self.m_group != l_original.m_group:
            l_original.copy()

    def store_db(self):
        """
        Stores a file object directly to DB, bypassing buffers (no longer used)

        :return: Nothing
        """
        if g_verbose:
            print('      store() :', self.m_path_suffix)

        l_cursor_write = FileHandler.cm_db_connection.cursor()
        try:
            l_cursor_write.execute("""
                insert into "TB_FILE"(
                    "TX_FILE_NAME", 
                    "TX_FILE_PATH", 
                    "N_LENGTH", 
                    "DT_LAST_MOD", 
                    "S_GROUP", 
                    "S_OWNER", 
                    "S_PERMISSIONS")
                    values( %s, %s, %s, %s, %s, %s, %s);
                """, (
                    self.m_filename,
                    self.m_full_path,
                    self.m_size,
                    self.m_lmd,
                    self.m_group,
                    self.m_owner,
                    oct(self.m_mode)
            ))

            FileHandler.cm_db_connection.commit()
        except Exception as e:
            FileHandler.cm_db_connection.rollback()

            print('DB ERROR:', repr(e))
            print(l_cursor_write.query)

            sys.exit(0)
        finally:
            # release DB objects once finished
            l_cursor_write.close()

    @classmethod
    def scan_dir(cls, p_root, p_operator, p_phase):
        """
        Do a full scan of a tree (starting with p_root) and applies the function p_operator to each file object.
        Used for both first and second scan

        :param p_root: Starting point (and prefix)
        :param p_operator: Function to be applied to each created file
        :param p_phase: Single letter (S, D or B) indicating the phase
        :return: Nothing
        """
        global g_err_presence, g_err_log_file

        # current process info for memory data
        l_process = psutil.Process(os.getpid())

        # list of prefixes to ignore (fed from .dbignore files)
        l_ignore_prefixes = []

        # walk the tree
        for l_dir, _, l_files in os.walk(p_root):
            # print directory info
            if not g_silent:
                print(
                    '{2} [{0:,.2f} Mb {1:,.2f} kF]'.format(
                        l_process.memory_info().rss/(1024*1024),
                        FileHandler.get_count() / 1000,
                        p_phase),
                    l_dir
                )

            # load .dbignore if there is one
            if '.dbignore' in l_files:
                with open(os.path.join(l_dir, '.dbignore'), 'r') as l_fin:
                    for l_line in l_fin.readlines():
                        l_line = l_line.strip()
                        if len(l_line) > 0:
                            l_ignore_prefixes.append(os.path.join(l_dir, l_line))

                if not g_silent:
                    print('   l_ignore_prefixes:', repr(l_ignore_prefixes))

            # ignore directory if prefix in l_ignore_prefixes
            l_ignore = False
            for l_prefix in l_ignore_prefixes:
                if re.match(r'^{0}'.format(l_prefix), l_dir):
                    if not g_silent:
                        print('   Ignored: {0}'.format(l_dir))
                    l_ignore = True
                    break

            if l_ignore:
                continue

            if g_verbose:
                print('   [F]', l_files)

            # Lists files in directory and apply appropriate operator on each of them
            for l_file_name in l_files:
                l_full_file = os.path.join(l_dir, l_file_name)

                if os.path.islink(l_full_file):
                    if g_verbose:
                        print('   SYMLINK: ', l_full_file)
                else:
                    if g_verbose:
                        print('   l_full_file: ', l_full_file)

                    try:
                        # create file object and apply operator
                        l_file = FileHandler(l_full_file)

                        p_operator(l_file)
                    except Exception as e:
                        g_err_presence = True
                        g_err_log_file.write(
                            'FileHandler creation failure on: {0}:\nErr: {1}\nTraceback:\n{2}\n----------\n'.format(
                                l_full_file, repr(e), traceback.format_exc()
                            ))

    @classmethod
    def log_action(cls, p_type, p_path1, p_path2):
        """
        Log an action to TB_ACTION (through buffer)

        :param p_type: 'D' Delete file ('Y' if failure), 'C' Copy file ('Z' if failure),
            'F' Delete empty folder ('W' if failure)
        :param p_path1: First path (used by all actions)
        :param p_path2: Second path (used only by C and B)
        :return: Nothing
        """

        cls.cm_db_buffer_action.write('{0}\t{1}\t{2}\t{3}\n'.format(
            cls.cm_cycle_uuid,
            p_type,
            p_path1.replace('\\', r'\\'),
            '' if p_path2 is None else p_path2.replace('\\', r'\\')
        ))

    @classmethod
    def log_action_db(cls, p_type, p_path1, p_path2):
        """
        Log action directly to the DB (no buffers)

        :param p_type: see log_action()
        :param p_path1: see log_action()
        :param p_path2: see log_action()
        :return: Nothing
        """
        l_cursor_write = FileHandler.cm_db_connection.cursor()
        try:
            l_cursor_write.execute("""
                insert into "TB_ACTION"(
                    "ID_CYCLE",
                    "S_ACTION_TYPE", 
                    "TX_PATH1", 
                    "TX_PATH2")
                    values( %s, %s, %s);
                """, (
                    cls.cm_cycle_uuid,
                    p_type,
                    p_path1,
                    p_path2
            ))

            FileHandler.cm_db_connection.commit()
        except Exception as e:
            FileHandler.cm_db_connection.rollback()

            print('DB ERROR:', repr(e))
            print(l_cursor_write.query)

            sys.exit(0)
        finally:
            # release DB objects once finished
            l_cursor_write.close()

    @classmethod
    def do_backup(cls, p_from, p_to):
        """
        Do a full cycle backup:

        1. Scan the source tree and create file objects in RAM
        2. Do a dry run of the second scan for statistics purposes
        3. Scan the destination tree and decides which files must be copied from the source or deleted
        4. Copy the remaining files from source to destination
        5. Remove empty directories in the destination

        During step 3 as the files are processed from the destination, the corresponding source file objects (if any)
        are removed from in-memory storage. As a result, the source files objects which remain for step 4. are those
        which were never referenced during step 3., i.e. those files present in the source but not the destination
        and which hence need to be copied there

        :param p_from: Source
        :param p_to: Destination
        :return: Nothing
        """
        global g_verbose

        # scan operator for the first scan (step 1.)
        def save_mem(p_file):
            p_file.store_mem()

        # scan operator for the second scan (steps 2. and 3.)
        def compare_file(p_file):
            p_file.compare()

        # make sure the class is in a pristine state
        cls.reset()

        # create the cycle's UUID
        cls.cm_cycle_uuid = uuid.uuid1()

        # store UUID into TB_CYCLE
        cls.open_connection()
        l_cursor_write = cls.cm_db_connection.cursor()
        try:
            l_cursor_write.execute("""
                insert into "TB_CYCLE"("ID_CYCLE")
                    values( %s );
                """, (str(cls.cm_cycle_uuid),)
                                   )

            cls.cm_db_connection.commit()
        except Exception as e:
            cls.cm_db_connection.rollback()

            print('DB ERROR:', repr(e))
            print(l_cursor_write.query)

            sys.exit(0)
        finally:
            # release DB objects once finished
            l_cursor_write.close()

        if g_verbose:
            print('+++++++ INITIAL SCAN ++++++++++')

        cls.set_prefix(p_from)
        cls.clear_base()
        cls.scan_dir(p_from, save_mem, 'S')

        l_total_count = len(cls.cm_mem_store)

        if g_verbose:
            print('+++++++ DRY RUN ++++++++++')

        cls.cm_dry_run = True
        cls.set_prefix(p_to)
        cls.scan_dir(p_to, compare_file, 'D')
        l_new_count = len(cls.cm_mem_store)
        cls.copy_remaining()

        # generate the stats display block at the end of the dry run
        l_stats = 'Total count     : {0:,d}\n'.format(l_total_count)
        l_stats += 'Total size      : {0:,.2f} Mb\n'.format(cls.cm_total_size / (1024 * 1024))
        l_stats += 'Copied count    : {0:,d}\n'.format(cls.cm_copied_count)
        l_stats += '   of which new : {0:,d}\n'.format(l_new_count)
        l_stats += 'Copied size     : {0:,.2f} Mb\n'.format(cls.cm_copied_size / (1024 * 1024))
        l_stats += 'Deleted count   : {0:,d}\n'.format(cls.cm_deleted_count)
        l_stats += 'Deleted size    : {0:,.2f} Mb\n'.format(cls.cm_deleted_size / (1024 * 1024))

        if not g_dry_run:
            # decide to do the Empty dir removal if at least one file was deleted
            l_do_empty_dirs = (cls.cm_deleted_count > 0)

            if g_verbose:
                print('+++++++ BACKUP ++++++++++')

            # temp buffer to in mem dict copy after dry run
            cls.cm_dry_run = False
            cls.cm_mem_store = cls.cm_mem_tmp
            cls.cm_mem_tmp = dict()
            cls.scan_dir(p_to, compare_file, 'B')
            cls.copy_remaining()
            if l_do_empty_dirs:
                cls.delete_empty_dirs()

        if not g_silent:
            print(l_stats)

        # make sure nothing is left in the buffers
        cls.purge()

        # Update end time into TB_CYCLE
        l_cursor_write = cls.cm_db_connection.cursor()
        try:
            l_cursor_write.execute("""
                update "TB_CYCLE"
                    set "DT_END" = %s
                where "ID_CYCLE" = %s;
                """, (datetime.datetime.now(), str(cls.cm_cycle_uuid))
                                   )

            cls.cm_db_connection.commit()
        except Exception as e:
            cls.cm_db_connection.rollback()

            print('DB ERROR:', repr(e))
            print(l_cursor_write.query)

            sys.exit(0)
        finally:
            # release DB objects once finished
            l_cursor_write.close()

        # Add 100 first actions to l_stats
        l_cursor_read = cls.cm_db_connection.cursor()
        try:
            l_cursor_read.execute(
                """
                    select * from "TB_ACTION"
                    where "ID_CYCLE" = %s
                    order by "ID_ACTION"
                    limit 100;
                """, (str(cls.cm_cycle_uuid),)
            )

            l_stats += '\nFirst 100 operations:\n'
            l_row_count = 0
            for _, l_type, l_p1, l_p2, _ in l_cursor_read:
                if l_p2 is None or l_p2 == 'None':
                    l_stats += '[{0:3}] {1} {2}\n'.format(l_row_count, l_type, l_p1)
                else:
                    l_stats += '[{0:3}] {1} {2} --> {3}\n'.format(l_row_count, l_type, l_p1, l_p2)
                l_row_count += 1
        except Exception as e:

            print('DB ERROR:', repr(e))
            print(l_cursor_read.query)

            sys.exit(0)
        finally:
            # release DB objects once finished
            l_cursor_read.close()

        cls.close_connection()

        return l_stats

    @classmethod
    def delete_empty_dirs(cls):
        """
        Scan the destination tree for empty directories and deletes them. Re-do until no deletion is made,
        to ensure all are deleted. Logs the action as 'F' if successful, 'W' otherwise.

        :return: Nothing
        """

        while True:
            l_delenda = []
            for l_root, l_dirs, l_files in os.walk(cls.cm_prefix):
                if len(l_dirs) == 0 and len(l_files) == 0:
                    l_delenda.append(l_root)

            if g_verbose:
                print('l_delenda :', l_delenda)

            if len(l_delenda) == 0:
                break

            for l_dir in l_delenda:
                # subprocess.call(['sudo', 'rmdir', l_dir])
                # FileHandler.log_action('F', l_dir, None)
                if FileHandler.execute_command(['sudo', 'rmdir', l_dir]):
                    FileHandler.log_action('F', l_dir, None)
                else:
                    FileHandler.log_action('W', l_dir, None)

    @classmethod
    def send_mail(cls, p_from, p_to, p_subject, p_text, p_attachment_path=None):
        """
        SMTP mail sending utility.

        :param p_from: message originator
        :param p_to: message recipient(s)
        :param p_subject: subject line
        :param p_text: message text
        :param p_attachment_path: optional file to attach
        :return: Nothing
        """
        global g_err_log_file

        l_text = re.sub('[ \t]+', ' ', p_text)
        l_text = re.sub('^[ \t\r\f\v]+', '', l_text, flags=re.MULTILINE)

        l_mime_msg = MIMEMultipart()
        l_mime_msg['From'] = p_from
        l_mime_msg['To'] = p_to
        l_mime_msg['Subject'] = p_subject

        l_mime_msg.attach(MIMEText(l_text))

        if p_attachment_path is not None:
            # The two lines below are specific of this project (disk io pb --> use of StringIO for g_err_log_file)
            # for reuse, the code commented out below should be used.
            l_part = MIMEApplication(g_err_log_file.getvalue(), Name=os.path.basename(p_attachment_path))
            l_mime_msg.attach(l_part)

            # with open(p_attachment_path, "rb") as l_att:
            #     l_part = MIMEApplication(l_att.read(), Name=os.path.basename(p_attachment_path))
            #     l_mime_msg.attach(l_part)

        l_message = l_mime_msg.as_string()
        # numeric value indicating the steps in the authentication process, for debug purposes
        l_step_passed = 0
        try:
            if g_amazonSmtp:
                # Amazon AWS/SES

                # smtp client init
                l_smtp_obj = smtplib.SMTP(
                    host=g_smtpServer,
                    port=587,
                    timeout=10)
                l_step_passed = 101

                # initialize TLS connection
                l_smtp_obj.starttls()
                l_step_passed = 102
                l_smtp_obj.ehlo()
                l_step_passed = 103

                # authentication
                l_smtp_obj.login(g_sesUserName, g_sesPassword)
                l_step_passed = 104
            elif g_gmailSmtp:
                # Gmail / TLS authentication
                # Also used for Smtp2Go

                # smtp client init
                l_smtp_obj = smtplib.SMTP(g_smtpServer, 587)
                l_step_passed = 201

                # initialize TLS connection
                l_smtp_obj.starttls()
                l_step_passed = 202
                l_smtp_obj.ehlo()
                l_step_passed = 203

                # authentication
                l_smtp_obj.login(g_mailSender, g_mailSenderPassword)
                l_step_passed = 204
            else:
                l_smtp_obj = smtplib.SMTP(g_smtpServer)

            # sending message
            l_smtp_obj.sendmail(g_mailSender, g_mailRecipients, l_message)
            l_step_passed = 99

            # end TLS session (Amazon SES / Gmail)
            if g_amazonSmtp or g_gmailSmtp:
                l_smtp_obj.quit()
        except smtplib.SMTPException as l_exception:
            # if failure, stores the message in a separate file
            print('Email sending error (SMTPException) :', repr(l_exception))
            print('l_step_passed :', l_step_passed)
            print(l_message)
        except Exception as e:
            print('Email sending error :', repr(e))
            print(l_message)


# ---------------------------------------------------- Main section ----------------------------------------------------
if __name__ == "__main__":
    l_parser = argparse.ArgumentParser(description='Daily backup v. IV')
    l_parser.add_argument('-v', help='Verbose', action='store_true', default=False)
    l_parser.add_argument('-s', help='Silent - no messages at all', action='store_true', default=False)
    l_parser.add_argument('-d', help='Dry-run only', action='store_true', default=False)
    l_parser.add_argument('--test-email', help='Send a test e-mail', action='store_true', default=False)
    l_parser.add_argument('--daily', help='Perform only daily backup', action='store_true', default=False)
    l_parser.add_argument('--show-delete', help='Display path of deleted files', action='store_true', default=False)
    l_parser.add_argument('--db-check', help='Test connection to DB server', action='store_true', default=False)
    l_parser.add_argument('--no-shutdown', help='Do not perform shutdown after execution',
                          action='store_true', default=False)

    # dummy class to receive the parsed args
    class C:
        def __init__(self):
            self.v = False
            self.s = False
            self.d = False
            self.daily = False
            self.show_delete = False
            self.no_shutdown = False
            self.test_email = False
            self.db_check = False

    # do the argument parse
    c = C()
    l_parser.parse_args()
    parser = l_parser.parse_args(namespace=c)

    g_verbose = c.v
    g_silent = c.s
    g_dry_run = c.d
    g_show_delete = c.show_delete

    # testing
    # l_original_prefix = '/home/fi11222/disk-partage/Dev/FileHandler/Test/Original'
    # l_backup_prefix = '/home/fi11222/disk-partage/Dev/FileHandler/Test/Backup'
    # g_verbose = True

    if not g_silent:
        print('+------------------------------------------------------------+')
        print('| File Backup and system shutdown                            |')
        print('|                                                            |')
        print('| file_handler.py                                            |')
        print('|                                                            |')
        print('| v. 1.0 - 22/05/2018                                        |')
        print('| Prod.  - 29/05/2018                                        |')
        print('| v. 1.1 - 21/09/2018 Enhanced error handling                |')
        print('| v. 1.2 - 05/12/2018 Better logging (TB_CYCLE)              |')
        print('| v. 1.3 - 15/05/2020 cmd line options                       |')
        print('| v. 1.4 - 13/10/2022 test e-mail option                     |')
        print('| v. 1.5 - 15/10/2023 DB check option                        |')
        print('+------------------------------------------------------------+')

        print('Verbose        :', g_verbose)
        print('Silent         :', g_silent)
        print('Dry-run only   :', g_dry_run)
        print('Show deleted   :', g_show_delete)
        print('g_err_log_path :', g_err_log_path)

    # g_err_log_file = open(g_err_log_path, "w")
    g_err_log_file = io.StringIO()
    g_err_log_file.write('Errors:\n')

    if c.db_check:
        FileHandler.open_connection(p_test=True)
    elif c.test_email:
        FileHandler.send_mail(g_mailSender, g_mailSender, '[Daily Backup Test Email]', 'This is a test')
    else:
        l_msg = 'file_handler.py\n\n'
        if g_dry_run:
            l_msg += '-- Dry run only --\n\n'
        try:
            # daily backup
            l_msg += '--------- Daily Backup ---------\n\n'
            l_stats_msg = FileHandler.do_backup('/home/fi11222/disk-share', '/home/fi11222/disk-backup/Partage')
            l_msg += l_stats_msg + '\n'

            # do weekly backup on thursday
            if datetime.datetime.today().weekday() == 0 and not c.daily:
                l_msg += '--------- Weekly Backup ---------\n\n'
                l_stats_msg = FileHandler.do_backup('/home/fi11222/disk-share', '/home/fi11222/disk-LTStore/Partage')
                l_msg += l_stats_msg + '\n'
        except Exception as e0:
            l_msg += 'High level Python Error:\n' + repr(e0) + '\n' + traceback.format_exc()

        # shutting down the system
        if not c.no_shutdown:
            if not g_silent:
                print('Shutting down in 5 minute')
            print('Shutdown return value:', FileHandler.execute_command('/sbin/shutdown -P +5', p_shell=True))

        if not g_silent:
            print('Sending e-mail ...')
        if g_err_presence:
            l_msg += '*** THERE ARE ERRORS ***\n'
            print('*** THERE ARE ERRORS ***')
            FileHandler.send_mail(g_mailSender, g_mailSender, '[Daily Backup]', l_msg, g_err_log_path)
        else:
            FileHandler.send_mail(g_mailSender, g_mailSender, '[Daily Backup]', l_msg)

        if g_show_delete and not g_silent:
            print('*** Deleted files ***')
            for f in g_deleted_files:
                print(f)

        if not g_silent:
            print('*** END ***')
