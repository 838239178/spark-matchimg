import io

from modules.app import Application
from modules.hbase import get_client
from modules.logger import getlogger
from .pixel_map import PixelMap

from .base_service import BaseService
from PIL import Image

LOG = getlogger(__name__)


class StoreService(BaseService):

    _table_name: str
    _family: str

    def __init__(self, name, fam) -> None:
        self._table_name, self._family = name, fam

    def _dump_log(self, px: PixelMap):
        LOG.info("put hbase data %s", px.key())
        res = px.dumps(self._family)
        return res

    def run(self, app: Application):
        ctx = app.ctx
        rdd = ctx.binaryFiles("/images") \
            .map(lambda tp: (tp[0].split('/')[-1], io.BytesIO(tp[1])))\
            .mapValues(lambda v: Image.open(v).__array__())\
            .map(lambda tp: (tp[0], PixelMap(tp[1], name=tp[0])))\
            .flatMapValues(self._dump_log)
        get_client().write_rdd(self._table_name, rdd)
