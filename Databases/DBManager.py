
import sqlite3
from threading import Lock

class Database(object):
    """A SQLite3 Database
    """
    def __init__(self, db_filename):
        """
        :param db_filename: Filename of SQLite3 DB (":memory:" to create a temp DB in RAM)
        """
        self.db_filename = db_filename
        self.db = sqlite3.connect(db_filename, check_same_thread=False)
        self.write_lock = Lock()

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

    def write(self, sentence, variables=(), commit=True, lock_wait=True, lock_timeout=-1):
        """Run a Write SQL Query
        :param sentence: SQL Query to execute (string, no need for final ';')
        :param variables: tuple with values for the query (replace question marks in sentence) (default=empty)
        :param commit: if True, commit changes to DB (default=True)
        :param lock_wait: if True, wait until write lock gets unlocked (or timed out) (default=True)
        :param lock_timeout: specify a timeout for the write lock when waiting (default=-1 a.k.a. no timeout)
        :raises: TimeoutError if lock can't be acquired (due to timeout or no-blocking)
        """
        if not self.write_lock.acquire(blocking=lock_wait, timeout=lock_timeout):
            raise TimeoutError("Couldn't acquire DB write lock")
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
        if commit:
            self.commit()
        self.db.close()
