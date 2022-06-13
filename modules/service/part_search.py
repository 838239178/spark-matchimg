from modules import hbase
from modules.app import Application
from modules.logger import getlogger
from numpy import ndarray

from .base_service import BaseService
from .pixel_map import PixelMap

LOG = getlogger(__name__)


class PartSearchService(BaseService):
    name: str
    _target: ndarray
    table_name: str
    family: str

    def __init__(self, table: str, fam: str, name: str = "partsample/sample1.bmp"):
        self.name = name
        self.table_name, self.family = table, fam

    def _check_target(self):
        if self._target is None:
            raise BaseException("target pixel map doesn'texisted")

    def __seprate_data(self, data: ndarray):
        if self._target is None:
            raise BaseException("target pixel map doesn't existed")
        w, h = self._target.shape[:2]
        end_w, end_h = data.shape[0] - w, data.shape[1] - h
        for i in range(0, end_h):
            for j in range(0, end_w):
                yield data[i:i+h, j:j+w]

    def __compare_data(self, nd: ndarray) -> bool:
        if self._target is None:
            raise BaseException("target pixel map doesn'texisted")
        return (nd == self._target).all()

    def run(self, app: Application):
        ctx = app.ctx
        self._target = self._get_hdfs(app.hdfs_api, self.name)
        LOG.info("target %s size: %s", self.name, self._target.shape)
        res = hbase.get_client()\
            .spark_rdd(ctx, self.table_name, self.family)\
            .map(lambda tp: (tp[0], PixelMap.load(*tp).data()))\
            .flatMapValues(self.__seprate_data)\
            .filter(lambda tp: self.__compare_data(tp[1]))\
            .keys().distinct().collect()
        if len(res) == 0:
            LOG.error("could not find anything")
        else:
            LOG.info("matched image name %s", str(res))
