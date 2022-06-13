from abc import ABCMeta, abstractmethod
import io
from PIL import Image
from numpy import ndarray
import requests

from modules.app import Application


class BaseService(metaclass=ABCMeta):

    @abstractmethod
    def run(self, app: Application):
        pass

    def _get_hdfs(self, api: str, name: str) -> ndarray:
        resp = requests.get(f"{api}/{name}?op=OPEN&user.name=root")
        if resp.status_code == 200:
            return Image.open(io.BytesIO(resp.content)).__array__()
        else:
            msg = resp.json()[
                'RemoteException']['message'] if resp.headers['Content-Type'] else resp.text
            raise BaseException(
                f"[{resp.status_code}] get target error: {msg}")
