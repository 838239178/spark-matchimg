cmd/add-swap.sh 3G /swapfile
scp cmd/add-swap.sh root@slave1:~
scp cmd/add-swap.sh root@slave2:~
ssh slave1 /root/add-swap.sh 2G /swapfile
ssh slave2 /root/add-swap.sh 2G /swapfile