#!/usr/bin/python3
# -*- coding: utf-8 -*-

import re
import sys

from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import traceback

# from email.MIMEMultipart import MIMEMultipart
# from email.MIMEText import MIMEText

print('before :', sys.path)
sys.path.append('/home/fi11222/.local/lib/python3.6/site-packages')
print('after :', sys.path)

import os
import pwd
import grp
import datetime
import psycopg2
import psutil
import io
import subprocess
import smtplib

__author__ = 'Nicolas Reimen'

# psycopg2 installation
# pip install psycopg2

# ---------------------------------------------------- Globals ---------------------------------------------------------

g_verbose = False
g_silent = False

g_dbServer = 'localhost'
g_dbDatabase = 'File_Base'
g_dbUser = 'postgres'
g_dbPassword = '15Eyyaka'

g_timeZone = 'Asia/Calcutta'

g_mailSender = 'nicolas.reimen@gmail.com'
g_mailSenderPassword = '17Siratal'

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
    def open_connection(cls):
        """
        Open PostgreSQL connection & sets up buffers
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
                '"TB_FILE"',
                columns=(
                    '"TX_FILE_NAME"',
                    '"TX_FILE_PATH"',
                    '"N_LENGTH"',
                    '"DT_LAST_MOD"',
                    '"S_GROUP"',
                    '"S_OWNER"',
                    '"S_PERMISSIONS"',
                    '"S_EXTENSION"'
                )
            )
            FileHandler.cm_db_connection.commit()
        except Exception as e:
            FileHandler.cm_db_connection.rollback()

            if not g_silent:
                print('DB ERROR:', repr(e))

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
                    '"TB_ACTION"',
                    columns=(
                        '"S_ACTION_TYPE"',
                        '"TX_PATH1"',
                        '"TX_PATH2"'
                    )
                )
                FileHandler.cm_db_connection.commit()
            except Exception as e:
                FileHandler.cm_db_connection.rollback()

                if not g_silent:
                    print('DB ERROR:', repr(e))

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

            if not g_silent:
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
        cls.purge()
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
    def execute_command(cls, p_cmd):
        """
        Execute a command (passed as a list) and recovers the outputs. If stderr or stdout is not empty -->
        log the error
        :param p_cmd: the command (as a list)
        :return: False if something went wrong, True otherwise
        """
        # access to global variables
        global g_err_presence, g_err_log_file

        try:
            l_out = subprocess.Popen(p_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

            l_stdout, l_stderr = l_out.communicate()
            l_stdout = l_stdout.decode('utf-8').strip() if l_stdout is not None else ''
            l_stderr = l_stderr.decode('utf-8').strip() if l_stderr is not None else ''

            if len(l_stdout) > 0 or len(l_stderr) > 0:
                g_err_presence = True
                g_err_log_file.write('Command returned: {0}:\n{1}{2}----------\n'.format(
                    ' '.join(p_cmd),
                    'stdout:' + l_stdout + '\n' if len(l_stdout) > 0 else '',
                    'stderr:' + l_stderr + '\n' if len(l_stderr) > 0 else ''
                ))
                return False
            else:
                return True
        except Exception as e:
            g_err_presence = True
            g_err_log_file.write('Command did not return: {0}:\nErr: {1}\nTraceback:\n{2}\n----------\n'.format(
                ' '.join(p_cmd), repr(e), traceback.format_exc()
            ))
            return False

    def __init__(self, p_path):
        """
        Initialization of a file object from its (absolute) path.

        :param p_path: file path
        """
        l_info = os.stat(p_path)

        # absolute path
        self.m_full_path = p_path
        # just the name of the file
        self.m_filename = os.path.basename(p_path)
        self.m_path_suffix = re.sub('^' + FileHandler.cm_prefix + '/', '', p_path)

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

        FileHandler.cm_db_buffer.write('{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\n'.format(
            self.m_filename,
            self.m_path_suffix,
            self.m_size,
            self.m_lmd.strftime('%Y-%m-%d %H:%M:%S'),
            self.m_group,
            self.m_owner,
            oct(self.m_mode),
            self.m_extension
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
        if g_verbose:
            print('Deleting : ' + self.m_full_path)

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

                # subprocess.call(['sudo', 'mkdir', '-p', l_target_path])
                FileHandler.execute_command(['sudo', 'mkdir', '-p', l_target_path])

            # subprocess.call(['sudo', 'cp', '-p', self.m_full_path, l_target_file])
            # FileHandler.log_action('C', self.m_full_path, l_target_file)
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

            if not g_silent:
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

        # walk the tree
        for l_dir, _, l_files in os.walk(p_root):
            if not g_silent:
                print(
                    '{2} [{0:,.2f} Mb {1:,.2f} kF]'.format(
                        l_process.memory_info().rss/(1024*1024),
                        FileHandler.get_count() / 1000,
                        p_phase),
                    l_dir
                )

            if g_verbose:
                print('   [F]', l_files)

            # lists files in directory
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

        :param p_type: 'D' Delete file, 'C' Copy file, 'F' Delete empty folder, 'B' Backup cycle start
        :param p_path1: First path (used by all actions)
        :param p_path2: Second path (used only by C and B)
        :return: Nothing
        """

        cls.cm_db_buffer_action.write('{0}\t{1}\t{2}\n'.format(
            p_type, p_path1, p_path2
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
                    "S_ACTION_TYPE", 
                    "TX_PATH1", 
                    "TX_PATH2")
                    values( %s, %s, %s);
                """, (
                    p_type,
                    p_path1,
                    p_path2
            ))

            FileHandler.cm_db_connection.commit()
        except Exception as e:
            FileHandler.cm_db_connection.rollback()

            if not g_silent:
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

        if g_verbose:
            print('+++++++ INITIAL SCAN ++++++++++')

        cls.open_connection()
        FileHandler.log_action('B', p_from, p_to)

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
    print('+------------------------------------------------------------+')
    print('| File Backup and other stuff                                |')
    print('|                                                            |')
    print('| file_handler.py                                            |')
    print('|                                                            |')
    print('| v. 1.0 - 22/05/2018                                        |')
    print('| Prod.  - 29/05/2018                                        |')
    print('| v. 1.1 - 21/09/2018 Enhanced error handling                |')
    print('+------------------------------------------------------------+')

    # testing
    # l_original_prefix = '/home/fi11222/disk-partage/Dev/FileHandler/Test/Original'
    # l_backup_prefix = '/home/fi11222/disk-partage/Dev/FileHandler/Test/Backup'
    # g_verbose = True

    print('g_err_log_path: ', g_err_log_path)
    # g_err_log_file = open(g_err_log_path, "w")
    g_err_log_file = io.StringIO()
    g_err_log_file.write('Errors:\n')

    l_msg = 'file_handler.py\n\n'
    try:
        # daily backup
        l_msg = '--------- Daily Backup ---------\n\n'
        l_stats_msg = FileHandler.do_backup('/home/fi11222/disk-partage', '/home/fi11222/disk-backup/Partage')
        l_msg += l_stats_msg + '\n'

        # do weekly backup on thursday
        if datetime.datetime.today().weekday() == 0:
            l_msg += '--------- Weekly Backup ---------\n\n'
            l_stats_msg = FileHandler.do_backup('/home/fi11222/disk-partage', '/home/fi11222/disk-LTStore/Partage')
            l_msg += l_stats_msg + '\n'
    except Exception as e0:
        l_msg += 'High level Python Error:\n' + repr(e0) + '\n' + traceback.format_exc()

    # g_err_log_file.close()
    if not g_silent:
        print('Sending e-mail ...')
    if g_err_presence:
        l_msg += '*** THERE ARE ERRORS ***\n'
        print('*** THERE ARE ERRORS ***')
        FileHandler.send_mail(g_mailSender, g_mailSender, '[Daily Backup]', l_msg, g_err_log_path)
    else:
        FileHandler.send_mail(g_mailSender, g_mailSender, '[Daily Backup]', l_msg)
