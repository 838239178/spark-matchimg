import base64
import pickle
import numpy as np
from typing import Tuple

from numpy import ndarray


class PixelMap(object):

    _pixel_map: list[int]
    _name: str
    _data: ndarray
    number: Tuple[int, int]

    def __init__(self, data: ndarray, name="", px_map: list[int] = None, num=(0, 0)):
        self.number = num
        self._name = name
        self._data = data
        if px_map is not None:
            self._pixel_map = px_map
        else:
            self._pixel_map = [0]*256
            if data.size > 0:
                for i in np.nditer(data):
                    self._pixel_map[i] += 1

    def dumps(self, family: str) -> list[Tuple[str, list[str]]]:
        data = base64.b85encode(self._data.dumps()).decode()
        if len(data) > (1 << 32 - 1):
            raise BaseException(f"data length exceed limit: {len(data)}")
        mp = ','.join([str(x) for x in self._pixel_map])
        return [
            (self._name, [self._name, family, 'map', mp]),
            (self._name, [self._name, family, 'data', data])
        ]

    def pixel(self, num: int) -> int:
        return 0 if num < 0 or num > 255 else self._pixel_map[num]

    def key(self) -> bytes:
        return self._name.encode()

    def name(self) -> str:
        return self._name

    def data(self) -> ndarray:
        return self._data

    @staticmethod
    def load(key: str, data_dict: dict[str, str]):
        if not isinstance(data_dict, dict):
            raise BaseException("data_dict should be dict")
        # 图像二维数组
        if 'data' in data_dict:
            data = pickle.loads(base64.b85decode(data_dict['data'].encode()))
        else:
            data = np.array([])
        # 直方图
        if 'map' in data_dict:
            px_map = [int(x) for x in data_dict['map'].split(',')]
        else:
            px_map = []
        return PixelMap(data, key, px_map)

    def __eq__(self, __o: object) -> bool:
        return isinstance(__o, PixelMap) and self._pixel_map == __o._pixel_map

    def __str__(self) -> str:
        return str(self._pixel_map)
