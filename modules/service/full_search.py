from modules.logger import getlogger
from modules.app import Application
from modules.hbase import get_client
from .base_service import BaseService
from .pixel_map import PixelMap

LOG = getlogger(__name__)


class FullSearchService(BaseService):

    name: str
    tb_name: str
    tb_fam: str
    _target: PixelMap

    def __init__(self, tb: str, name: str = "fullsample/sample1.bmp"):
        self.name = name
        self.tb_name, self.tb_fam = tb.split(':')

    def compare_pixel(self, img: PixelMap) -> bool:
        if self._target is None:
            raise BaseException("target pixel map doesn't existed")
        return self._target == img

    def run(self, app: Application):
        ctx = app.ctx
        self._target = PixelMap(self._get_hdfs(app.hdfs_api, self.name))
        LOG.info("target %s px map: %s", self.name, self._target)
        res = get_client().spark_rdd(ctx, self.tb_name, self.tb_fam)\
            .map(lambda tp: (tp[0], PixelMap.load(*tp)))\
            .values()\
            .filter(self.compare_pixel)\
            .keys().collect()
        if len(res) == 0:
            LOG.error("could not find anything")
        else:
            LOG.info("matched image name %s", res)
