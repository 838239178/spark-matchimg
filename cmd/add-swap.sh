if [ $# != 2 ]; then
    echo "add-swap.sh [size(1G)] [fileName]"
    exit 1
fi

fallocate -l $1 $2 && \
chmod 600 $2 && \
mkswap $2 && \
swapon $2 && \
swapon -s