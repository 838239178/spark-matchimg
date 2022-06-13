import io

import requests
from modules.app import Application
from modules.hbase import get_client
from modules.logger import getlogger
from numpy import ndarray
from PIL import Image

from .base_service import BaseService
from .pixel_map import PixelMap

LOG = getlogger(__name__)

SEP_UNIT = 40


class FalsifyCheckService(BaseService):

    name: str
    _table_name: str
    _target: ndarray
    _family: str

    def __init__(self, table: str, family: str, name: str = "falsifysample/sample1.bmp") -> None:
        self.name = name
        self._table_name, self._family = table, family

    def __seprate_px(self, px: PixelMap):
        data = px.data()
        end = int(data.shape[0] / SEP_UNIT)
        for i in range(0, end):
            for j in range(0, end):
                y, x = i * SEP_UNIT, j * SEP_UNIT
                yield PixelMap(data[y:y+SEP_UNIT, x:x+SEP_UNIT], px_map=[], num=(y, x))

    def _check_target(self):
        if self._target is None:
            raise BaseException("target pixel map doesn'texisted")

    def _match_deg(self, px: PixelMap) -> int:
        self._check_target()
        y, x = px.number
        part = self._target[y:y+SEP_UNIT, x:x+SEP_UNIT]
        res = part - px.data()
        return (res * res).sum()

    def get_falsify_loc(self, data: ndarray):
        self._check_target()
        sub = self._target - data
        start, end, flag = (0, 0), (0, 0,), True
        for i in range(0, sub.shape[0]):
            for j in range(0, sub.shape[1]):
                if sub[i][j] != 0:
                    end = (i, j)
                    start, flag = (i, j), False if flag else start, flag

        return (*start, *end)

    def run(self, app: Application):
        cli = get_client()
        self._target = self._get_hdfs(app.hdfs_api, self.name)
        max_match = cli.spark_rdd(app.ctx, self._table_name, self._family)\
            .map(lambda tp: (tp[0], PixelMap.load(*tp)))\
            .flatMapValues(self.__seprate_px)\
            .mapValues(self._match_deg)\
            .reduceByKey(lambda a, b: a+b)\
            .reduce(lambda tp1, tp2: tp2 if tp2[1] < tp1[1] else tp1)
        LOG.info("best match %s", str(max_match))
        img = self._get_hdfs(app.hdfs_api, f"images/{max_match[0]}")
        loc = self.get_falsify_loc(img)
        LOG.info("falsify location is %s, size (%d,%d)", loc, loc[2]-loc[0], loc[3]-loc[1])
