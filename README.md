# 💡 **About**

This is a dynamic connection pool for [mysqlclient](https://github.com/PyMySQL/mysqlclient) connector and size of it grows as it requires. Extra connections will be terminated automatically if they're no longer needed.

The connection pool won't check the connectivity state of the connections before passing them to the user because in any time is still possible for the connection to drop in middle of the query. The user itself should watch for the disconnections.

The connection pool is thread-safe and can be shared on multithreaded context as long as the indivisual connection object not shared between the threads. However individual pool instances are required for different processes.

# 🔌 **Installation**

```bash
pip install mysqlclient-pool
```

# 📋 **How to Use**

Instantiating the connection pool. The pool also can be instantiated as a context manager using `with` statement.

```python
from mysqlclient_pool import ConnectionPool
from MySQLdb._exceptions import OperationalError, ProgrammingError


try:
    pool = ConnectionPool(
        {
            "unix_socket": "/var/run/mysqld/mysqld.sock",
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "password": "...",
            "database": "mysql"
        },
        size=20,
        timeout=10
    )
except TimeoutError:
    # Couldn't connect to the database server.
    # MySQL server service can be restarted in here if it's down.
    pass
```

Acquiring a `cursor` object from the pool. `fetch()` method commits or rollbacks the changes by default.

```python

try:
    with pool.fetch() as cursor:
        cursor.execute("SELECT DATABASE()")
        print(cursor.fetchone())
except (OperationalError, ProgrammingError):
    # Handling MySQL errors
    pass
except pool.OverflowError:
    # The pool can't provide a connection anymore
    # because maximum permitted number of simultaneous
    # connections is exceeded.
    # `max_connections` variable of MySQL server configuration
    # can be tweaked to change the behaviour.
    pass
except pool.DrainedError:
    # The pool can't provide a connection anymore
    # because it can't access the database server.
    pass
```

`connection` object also can be accessed if needed. But any changes to connection should be reverted when returning the connection back to the pool.

```python
with pool.fetch() as cursor:
    try:
      cursor.connection.autocommit(True)
      cursor.execute("INSERT INTO ...")
      cursor.execute("UPDATE ...")
      cursor.execute("DELETE FROM ...")
    finally:
      cursor.connection.autocommit(False)
```

Closing the pool when it's not needed anymore.

```python
pool.close()
```

# 🔧 **API**

- _class_ **`mysqlclient_pool.ConnectionPool`**

  - _method_ **`__init__(config: dict, size: int = 10, timeout: int = 5) -> None`**

    - _parameter_ **`config`**:  
      The keyword paramaters for creating the connection object.

    - _parameter_ **`size`**:  
      The minimum number of the connections in the pool.

    - _parameter_ **`timeout`**:  
      The time in seconds to wait for initiating the connection pool if the database server is unavailable.

    - _exception_ **`TimeoutError`**:  
      When Unable to fill up the connection pool due to inability to connect to the database server.

  - _method_ **`close() -> None`**  
    Closes the connection pool and disconnects all the connections.

  - _method_ **`fetch(auto_manage: bool = True, cursor_type: MySQLdb.cursors.Cursor | MySQLdb.cursors.DictCursor = MySQLdb.cursors.Cursor) -> collections.abc.Generator[MySQLdb.cursors.Cursor | MySQLdb.cursors.DictCursor, None, None]`**  
    Returns a cursor object from a dedicated connection.

    This is a context manager which pulls a connection from the pool and generates a cursor object from it and returns it to the user and at the end, if the connection hasn't disconnected in the way, closes the cursor and returns the connection back to the pool.

    - _parameter_ **`auto_manage`**:  
      If `True` provided, if no unhandled exception raised in the enclosed block, commits the current transaction upon completion of the block or rollbacks the transaction on an unhandled exception.

    - _parameter_ **`cursor_type`**:  
      Type of the cursor.

    - _exception_ **`RuntimeError`**:  
      When called after closing the connection pool.

    - _exception_ **`ConnectionPool.DrainedError`**:  
      When there's no connection available in the pool and unable to initiate new connections due to inability to connect to the database server.

    - _exception_ **`ConnectionPool.OverflowError`**:  
      When unable to initiate new connections due to maximum permitted number of simultaneous connections is exceeded.

  - _property_ **`capacity: int`**  
    Amount of idle connections present in the connection pool.
