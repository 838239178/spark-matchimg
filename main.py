import sys
from modules.app import Application
from modules.logger import getlogger
from modules.hbase import set_client as init_hbase, HbaseClient
from modules.service import StoreService, FullSearchService, PartSearchService, FalsifyCheckService


log = getlogger(__name__)

app = Application(
    app_name="ImageSearch5",
    host="master",
    master="yarn",
    pyfile="hdfs:/pkg/modules.zip",
    log_level="ERROR"
)

init_hbase(HbaseClient(host='master'))

mode = sys.argv[1]

if mode == 'store':
    log.info("run store service")
    serv = StoreService("pxmap", "info")
    serv.run(app)
elif mode == 'full':
    log.info("run full search service")
    serv = FullSearchService("pxmap:info", sys.argv[2])
    serv.run(app)
elif mode == 'part':
    log.info("run part search service")
    serv = PartSearchService("pxmap", "info:data", sys.argv[2])
    serv.run(app)
elif mode == 'falsify':
    log.info("run falsify check service")
    serv = FalsifyCheckService("pxmap", "info:data", sys.argv[2])
    serv.run(app)
else:
    log.error("command [store|full|part|falsify], input %s", sys.argv[1])
