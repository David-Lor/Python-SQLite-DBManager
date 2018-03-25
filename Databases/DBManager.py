
#Native libraries
import sqlite3
import atexit
from threading import Lock
from datetime import datetime


class Database(object):
    """A SQLite3 Database
    """
    def __init__(self, db_filename, datetime_format="%y-%m-%d %H:%M:%S"):
        """
        :param db_filename: Filename of SQLite3 DB (":memory:" to create a temp DB in RAM)
        :param datetime_format: Datetime to string format for curdate() method (default: "%y-%m-%d %H:%M:%S")
        """
        self.db_filename = db_filename
        self.datetime_format = datetime_format
        self.db = sqlite3.connect(db_filename, check_same_thread=False)
        self.write_lock = Lock()
        @atexit.register
        def atexit_f():
            self.close()

    def read(self, sentence, variables=(), fetchall=True, single_column=False):
        """Run a Read SQL Query
        :param sentence: SQL Query to execute (string, no need for final ';')
        :param variables: tuple with values for the query (replace question marks in sentence) (default=empty)
        :param fetchall: if True, return multiple values (default=False)
        :param single_value: if True, only return a single column, only when fetchall=False (default=True)
        :return: Data result with fetchone/fetchmany as tuple (except single_value=True)
        """
        try:
            cursor = self.db.cursor()
            result = cursor.execute(sentence, variables)
            if fetchall:
                return result.fetchall()
            elif single_column: #fetchone, single column
                return result.fetchone()[0]
            else: #fetchone, all columns
                return result.fetchone()
        finally:
            cursor.close()

    def write(self, sentence, variables=(), commit=True, lock_wait=True, lock_timeout=-1, ignore_timeout_error=False):
        """Run a Write SQL Query
        :param sentence: SQL Query to execute (string, no need for final ';')
        :param variables: tuple with values for the query (replace question marks in sentence) (default=empty)
        :param commit: if True, commit changes to DB (default=True)
        :param lock_wait: if True, wait until write lock gets unlocked (or timed out) (default=True)
        :param lock_timeout: specify a timeout for the write lock when waiting (default=-1 a.k.a. no timeout)
        :param ignore_timeout_error: if True, do not raise TimeoutError exception when write_lock could not be acquired
        :raises: TimeoutError if lock can't be acquired (due to timeout or no-blocking)
        """
        if not self.write_lock.acquire(blocking=lock_wait, timeout=lock_timeout):
            if ignore_timeout_error:
                return
            raise TimeoutError("Could not acquire DB write lock")
        try:
            cursor = self.db.cursor()
            cursor.execute(sentence, variables)
            if commit:
                self.commit()
        finally:
            cursor.close()
            self.write_lock.release()

    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()

    def close(self, commit=False):
        #Close won't raise error even if DB was already closed
        if commit:
            self.commit()
        self.db.close()

    def curdate(self, dtformat=None):
        """Returns current date & time as string with a custom format.
        :param dtformat: Datetime format (default: None = datetime_format from Database constructor)
        """
        if dtformat is None:
            dtformat = self.datetime_format
        return datetime.now().strftime(dtformat)