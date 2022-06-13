if [[ $1 = 'test' ]]
then
    python3 main_test.py $2
else
    python3 main.py $*
fi

