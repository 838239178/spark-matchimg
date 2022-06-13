#variables
REQUIRE_FILE=/root/workspace/requirements.txt
MODULE_HOME=modules
MIRROR_PIP=https://pypi.mirrors.ustc.edu.cn/simple/

#modules upload
echo "package modules.."
zip -r -q modules.zip $MODULE_HOME

echo "upload modules.."
hdfs dfs -put -f modules.zip /pkg/modules.zip

if [[ $1 = "-d" ]]
then
    echo "use -i plz"
    exit 1
elif [[ $1 = "-i" ]]
then
    echo "install requirements to slaves.."
    scp $REQUIRE_FILE root@slave1:~ && \
    ssh slave1 pip install -r requirements.txt -i $MIRROR_PIP  
    scp $REQUIRE_FILE root@slave2:~ && \
    ssh slave2 pip install -r requirements.txt -i $MIRROR_PIP
fi

# clean
echo "clean zip files"
rm *.zip
