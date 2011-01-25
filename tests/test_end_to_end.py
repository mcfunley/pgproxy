from __future__ import with_statement
from contextlib import closing, contextmanager
from twisted.trial import unittest
import os
from twisted.python.procutils import which 
import subprocess
from pgproxy import _waitForServerUp
import pgproxy

def _import_psycopg2():
    try:
        import psycopg2
    except ImportError:
        return None
    return psycopg2
def _import_pypgsql():
    try:
        from pyPgSQL import PgSQL
    except ImportError:
        return None
    return PgSQL
psycopg2 = _import_psycopg2()
pypgsql = _import_pypgsql()

this_dir = os.path.realpath(os.path.dirname(__file__))


@contextmanager
def leaving_open(x):
    yield x


def testmethod(name, f, driver):
    def g(self, *args):
        if driver == 'psyco' and psycopg2 is None:
            raise unittest.SkipTest('psycopg2 is not available')
        elif driver == 'pypgsql' and pypgsql is None:
            raise unittest.SkipTest('pypgsql is not available')
        self.connect = getattr(self, 'connect_'+driver)

        self.execute("begin test '%s'" % name, results=False)
        try:
            return f(self, *args)
        finally:
            self.execute("rollback test '%s'" % name, results=False)
    g.__name__ = name
    return g


def setmethod(dct, k, v, driver):
    dct[k+'_'+driver] = testmethod(k+'_'+driver, v, driver)


class EndToEndMeta(type):
    def __new__(cls, name, bases, dct):
        for k, v in dct.items():
            if k.startswith('test') and callable(v):
                del dct[k]
                if pypgsql is not None:
                    setmethod(dct, k, v, 'pypgsql')
                if psycopg2 is not None:
                    setmethod(dct, k, v, 'psyco')

        return type.__new__(cls, name, bases, dct)


class EndToEndTests(unittest.TestCase):

    __metaclass__ = EndToEndMeta

    # postgres settings
    data_dir = None
    pg_port = 54321
    postgresRunning = False
    pglog = os.path.join(this_dir, 'pg.log')
    _pg_ctl = None

    proxyPort = 54320
    proxyRunning = False
    proxy = None
    proxyLog = os.path.join(this_dir, 'pgproxy.log')

    dbpassword_file = os.path.join(this_dir, 'testpw')
    _dbpassword = None
    
    def previouslyKnownAsSetUpClass(self):
        self.clearLogs()
        self.initializeDatabase()
        self.startPostgres()


    def previouslyKnownAsTearDownClass(self):
        if self.postgresRunning:
            self.pg_ctl('stop')        


    def setUp(self):
        self.previouslyKnownAsSetUpClass()
        self.createTestDatabase()
        self.startProxy()


    def tearDown(self):
        if self.proxy:
            self.proxy.stop()
        self.previouslyKnownAsTearDownClass()


    def clearLogs(self):
        for log in (self.pglog, self.proxyLog,):
            if os.path.isfile(log):
                os.unlink(log)


    def initializeDatabase(self):
        """
        Creates a scratch postgres database (using initdb) if one does not 
        already exist. 
        """
        self.data_dir = os.path.join(this_dir, 'data')
        if os.path.isdir(self.data_dir):
            return

        initdb = which('initdb')
        if not initdb:
            self.skipTests('Could not find initdb')
            return

        subprocess.Popen([
                initdb[0], '--pgdata=%s' % self.data_dir, 
                '--username=postgres', 
                '--pwfile=%s' % self.dbpassword_file,]).communicate()

        # need to set the permissions or PG will refuse to start. 
        os.chmod(self.data_dir, 0700)


    def createTestDatabase(self):
        script = os.path.join(this_dir, 'testdb.sql')
        psql = which('psql')
        if not psql:
            raise unittest.SkipTest('Could not locate psql')

        subprocess.Popen(
            [psql[0], '-U', 'postgres', '-p', str(self.pg_port), '-f', script],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()


    def dsn(self, direct, db):
        return ('host=127.0.0.1 port=%s user=postgres '
                'dbname=%s sslmode=disable' % (
                self.pg_port if direct else self.proxyPort, db))


    def connect_psyco(self, direct=False, db='test'):
        return psycopg2.connect(self.dsn(direct, db))


    def connect_pypgsql(self, direct=False, db='test'):
        # the dbapi2-compliant call doesn't work with this, but this does, 
        # i have no idea why. 
        return pypgsql.Connection(self.dsn(direct, db))


    def dbpassword(self):
        if not self._dbpassword:
            self._dbpassword = open(self.dbpassword_file, 'r').read()
        return self._dbpassword


    def startPostgres(self):
        """
        Starts a test postgres server instance.
        """
        if self.pg_ctl('start'):
            self.postgresRunning = True
        return self.postgresRunning


    @property
    def pg_ctl_command(self):
        """
        Returns the path to pg_ctl, or an empty string if it can't be found.
        """
        if self._pg_ctl:
            return self._pg_ctl

        pg = which('pg_ctl')
        if not pg:
            self.skipTests('Could not find pg_ctl')
            return ''

        self._pg_ctl = pg[0]
        return self._pg_ctl


    def pg_ctl(self, op):
        """
        Performs a pg_ctl operation. 
        """
        c = self.pg_ctl_command
        if not c:
            return False

        subprocess.Popen(
            [c, op, '-D', self.data_dir, '-l', self.pglog, 
             '-o', '"-p %s"' % str(self.pg_port)],
            stdout=subprocess.PIPE)
        return self.waitForServerUp('postgres', self.pg_port)


    def waitForServerUp(self, kind, port):
        """
        Waits for a server to start accepting socket connections. 
        """
        if _waitForServerUp(port):
            return True
        self.skipTests('Could not start %s server on port %s' % (kind, port))
        return False
        

    def skipTests(self, reason):
        """
        Marks all of the tests as skipped in the event that postgres can't be 
        started. 
        """
        def skipit(*args):
            raise unittest.SkipTest(reason)
        for m in [x for x in dir(self) if x.startswith('test')]:
            a = getattr(self, m)
            if callable(a):
                setattr(self, m, skipit)


    def startProxy(self):
        """
        Starts a pgproxy instance pointed at the test postgres instance. 
        """
        try:
            self.proxy = pgproxy.run(
                listenPort=self.proxyPort, 
                serverAddr=('localhost', self.pg_port),
                logfile=self.proxyLog)
        except AssertionError, e:
            self.skipTests(str(e))


    def execute(self, sql, results=True, conn=None):
        with (leaving_open(conn) if conn else closing(self.connect())) as c:
            cr = c.cursor()
            cr.execute(sql)
            if results:
                return cr.fetchall()


    def scalar(self, sql, conn=None):
        return self.execute(sql, conn=conn)[0][0]


    def test_select(self):
        self.assertEqual(self.scalar('select count(*) from foo;'), 2)


    def test_connection_stack(self):
        c1 = self.connect()
        c2 = self.connect()
        c2.close()
        self.assertEqual(self.scalar('select 5;', conn=c1), 5)
        self.assertEqual(
            self.scalar('select count(*) from foo;', conn=c1), 2)


    def test_committing(self):
        # this should be rewritten to savepoint usage. 
        with closing(self.connect()) as c:
            self.execute('insert into foo (x) values (42)', 
                         conn=c, results=False)
            c.commit()
        self.assertEqual(self.scalar('select count(*) from foo'), 3)

    def test_rolling_back(self):
        with closing(self.connect()) as c:
            self.execute('insert into foo(x) values (42)',
                         conn=c, results=False)
            c.rollback()
        self.assertEqual(self.scalar('select count(*) from foo'), 2)


    def test_activating_connections(self):
        # make sure that the right clients get the right responses 
        # when multiple clients are connected. 
        c1 = self.connect()
        c2 = self.connect()

        self.assertEqual(self.scalar('select 5', conn=c1), 5)
        self.assertEqual(self.scalar('select 6', conn=c2), 6)
        self.assertEqual(self.scalar('select 7', conn=c1), 7)

        c2.close()
        c1.close()
        

    def test_errors(self):
        try:
            self.scalar('select foo')
        except:
            pass
        else:
            self.fail('that should not have worked')

        # that should not fuck up subsequent queries
        self.assertEqual(5, self.scalar('select 5'))


    def test_big_response(self):
        with closing(self.connect()) as c:
            cr = c.cursor()
            cr.execute('select * from generate_series(0, 700)')
            n = 0
            while True:
                x = cr.fetchone()
                if x is None:
                    return
                self.assertEqual(n, x[0])
                n += 1

