from functools import wraps

from .client import HbaseClient


__def_cli: HbaseClient = None


def set_client(client: HbaseClient):
    global __def_cli
    __def_cli = client

def get_client() -> HbaseClient:
    if __def_cli is None:
        raise BaseException("init hbase first")
    return __def_cli


def def_hbase(func):
    """
    use default hbase client, inject client named 'hbase', do not use this name as your own param
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        with __def_cli.connection() as conn:
            return func(*args, hbase=conn, **kwargs)
    return wrapper


def use_hbase(client: HbaseClient):
    def hbase_client(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with client.connection() as conn:
                func(*args, hbase=conn, **kwargs)
        return wrapper
    return hbase_client
