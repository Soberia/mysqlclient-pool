from time import sleep
from threading import Lock
from collections import deque
from collections.abc import Generator
from contextlib import suppress, contextmanager

from MySQLdb import connect
from MySQLdb.cursors import Cursor, DictCursor
from MySQLdb.connections import Connection
from MySQLdb._exceptions import OperationalError

MYSQL_CON_COUNT_ERROR = 1040  # Too many connections error
MYSQL_CN_ERRORS = (2002, 2003)  # Socket and TCP connection errors
MYSQL_DC_ERRORS = (2006, 2013, 4031)  # Dropped, lost and inactive connection errors


class ConnectionPool:
    """MySQL database dynamic connection pool for the `mysqlclient` connector.

    The size of the pool is dynamic and grows as it requires.
    The minimum size is determined by the `size` parameter and new connections
    will be initiated if there's no connection available in the pool.
    When connections return to the pool, extra connections will be terminated.

    The connection pool won't check the connectivity state of the connections
    before passing them to the user because in any time is still possible
    for the connection to drop in middle of the query. The user itself should
    watch for the disconnections.

    The connection pool implemented in double-ended queue to reduce the
    risk of inactive connection drops by the database server. That means
    oldest connection always consume first.
    Inactive connection timeout controlled by `wait_timeout` variable of
    MySQL server configuration and if an inactive connection be used
    after this time, `ER_CLIENT_INTERACTION_TIMEOUT` error with `4031`
    error code will be thrown by the server.

    The connection pool is thread-safe and can be shared on multithreaded
    context as long as the individual connection object not shared between
    the threads. However individual pool instances are required for different
    processes.
    See https://peps.python.org/pep-0249/#threadsafety

    Args:
        `config`:
            The keyword parameters for creating the connection object.

        `size`:
            The minimum number of the connections in the pool.

        `timeout`:
            The time in seconds to wait for initiating the connection pool
            if the database server is unavailable.

        `fillup`:
            If `True` provided, fills up the connection pool up to the `size`
            parameter. Otherwise the connection pool is initially empty.

    Raises:
        ``TimeoutError``:
            When unable to fill up the connection pool due to inability
            to connect to the database server.

    Examples:
        Pinging the database server:

        >>> from getpass import getpass
        >>> with ConnectionPool(
        ...     {
        ...         "unix_socket": "/var/run/mysqld/mysqld.sock",
        ...         "host": "localhost",
        ...         "port": 3306,
        ...         "user": "root",
        ...         "password": getpass(),
        ...         "database": "mysql"
        ...     }
        ... ) as pool:
        ...     with pool.fetch() as cursor:
        ...         if cursor.execute("SELECT 1"):
        ...             cursor.fetchone()
        (1,)
    """

    class DrainedError(ConnectionError):
        """
        There's no connection available in the pool and unable to
        initiate new connections due to inability to connect to the
        database server.
        """

        def __init__(self) -> None:
            super().__init__(
                "There's no connection available in the connection pool."
                " unable to initiate new connections due to inability to"
                " connect to the database server."
            )

    class OverflowError(ConnectionRefusedError):
        """
        Unable to initiate new connections due to maximum permitted
        number of simultaneous connections is exceeded.

        `max_connections` variable of MySQL server configuration could
        be tweaked to change the behavior.
        """

        def __init__(self) -> None:
            super().__init__(
                "Unable to initiate new connections due to maximum"
                " permitted number of simultaneous connections is exceeded."
            )

    def __init__(
        self, config: dict, size: int = 10, timeout: int = 5, fillup: bool = True
    ) -> None:
        self.config = config
        self.size = size
        self._pool = deque()
        self._closed = None
        self._reload_id = 0
        self._reload_lock = Lock()
        if fillup:
            self._load(float(timeout))

    def __enter__(self) -> "ConnectionPool":
        return self

    def __exit__(self, *exception) -> None:
        self.close()

    def _connection(self) -> Connection:
        """
        Initiates a new connection to the database and
        returns the connection object.
        """
        try:
            return connect(**self.config)
        except OperationalError as error:
            if (error_code := error.args[0]) in MYSQL_CN_ERRORS:
                raise TimeoutError("Unable to connect to the database server.")
            elif error_code == MYSQL_CON_COUNT_ERROR:
                raise self.OverflowError()
            raise

    def _load(self, timeout: float = 0) -> None:
        """Fills the connection pool up to the ``self.size`` attribute."""
        _timeout = timeout
        while self.capacity < self.size:
            try:
                self._pool.append(self._connection())
            except TimeoutError:
                if _timeout > 0:
                    _timeout -= 0.1
                    sleep(0.1)
                else:
                    raise TimeoutError(
                        "Unable to fill up the connection pool due to inability"
                        " to connect to the database server"
                        f"{f' after {int(timeout)} seconds' if timeout else ''}."
                    )

    def _reload(self) -> None:
        """Refills the connection pool with new connections."""
        if self._reload_lock.acquire(blocking=False):
            try:
                if not self._closed:
                    self._reload_id += 1
                    self._drain()
                    self._load()
            finally:
                self._reload_lock.release()

    def _drain(self) -> None:
        """Closes all the connections in the connection pool."""
        for connection in self._pool:
            with suppress(Exception):
                connection.close()
        self._pool.clear()

    def _pull(self) -> Connection:
        """Returns a connection object from the connection pool.

        A new connection will be initiated if there's no
        connection available in the pool at the moment.
        """
        if self._closed:
            raise RuntimeError("The connection pool is closed.")
        elif self._reload_lock.locked():
            raise self.DrainedError()

        if self.capacity:
            with suppress(IndexError):
                return self._pool.popleft()

        try:
            return self._connection()
        except TimeoutError:
            raise self.DrainedError()

    def _release(self, connection: Connection) -> None:
        """Returns the given connection object back to the connection pool."""
        self._pool.append(connection)

        # Closing the extra connections
        while self.capacity > self.size:
            with suppress(Exception):
                (self._pool.popleft()).close()

    def _adjust(self) -> None:
        """
        TODO: Adjust ``self.size`` attribute automatically based on the usage.
        """
        raise NotImplementedError()

    def close(self) -> None:
        """Closes the connection pool."""
        with self._reload_lock:
            self._closed = True
            self._drain()

    @contextmanager
    def fetch(
        self, auto_manage: bool = True, cursor_type: Cursor | DictCursor = Cursor
    ) -> Generator[Cursor | DictCursor, None, None]:
        """Returns a cursor object from a dedicated connection.

        This is a context manager which pulls a connection from the pool and
        generates a cursor object from it and returns it to the user and at
        the end, if the connection hasn't disconnected in the way, closes the
        cursor and returns the connection back to the pool or on the other
        hand refills the entire pool with new connections.

        Args:
            `auto_manage`:
                If `True` provided, if no unhandled exception raised in the
                enclosed block, commits the current transaction upon completion
                of the block or rollbacks the transaction on an unhandled
                exception.

            `cursor_type`:
                Type of the cursor.

        Raises:
            ``RuntimeError``:
                When called after closing the connection pool.

            ``ConnectionPool.DrainedError``:
                When there's no connection available in the pool and unable to
                initiate new connections due to inability to connect to the
                database server.

            ``ConnectionPool.OverflowError``:
                When unable to initiate new connections due to maximum permitted
                number of simultaneous connections is exceeded.
        """
        # Storing the current reload ID. If this value changes during the
        # lifetime of this method, that means pool refilled with new connections
        # and further pool reloads should be avoided. Also current connection
        # is also probably disconnected and shouldn't be returned to the pool.
        reload_id = self._reload_id
        connection = cursor = disconnected = None
        try:
            try:
                connection = self._pull()
                yield (cursor := connection.cursor(cursor_type))
            except Exception as error:
                if auto_manage and connection is not None:
                    if not (
                        isinstance(error, OperationalError)
                        and error.args[0] in MYSQL_DC_ERRORS
                    ):
                        connection.rollback()
                raise
            else:
                if auto_manage:
                    connection.commit()
        except OperationalError as error:
            if error.args[0] in MYSQL_DC_ERRORS:
                # Connection is disconnected. All the other connections in the
                # pool also probably disconnected as well and the entire pool
                # should be refilled with new connections.
                #
                # If error is `CR_SERVER_GONE_ERROR` with `2006` error code,
                # all the other connections are also disconnected for sure.
                # If error is `CR_SERVER_LOST` with `2013` error code, then
                # only this connection is disconnected if error is result of
                # long data transmission timeout, otherwise all the other
                # connections are disconnected as well.
                # If error is `ER_CLIENT_INTERACTION_TIMEOUT` with `4031` error
                # code, all the connections in the pool are probably timed out.
                disconnected = True
                if reload_id == self._reload_id:
                    with suppress(Exception):
                        self._reload()
            raise
        finally:
            if connection is not None:
                close = False
                if self._closed:
                    close = True
                elif not disconnected:
                    if reload_id == self._reload_id:
                        if cursor is not None:
                            with suppress(Exception):
                                cursor.close()
                        self._release(connection)
                    else:
                        close = True

                if close:
                    with suppress(Exception):
                        connection.close()

    @property
    def capacity(self) -> int:
        """The amount of idle connections present in the connection pool."""
        return len(self._pool)

    @property
    def closed(self) -> bool:
        """The state of the connection pool."""
        return self._closed
