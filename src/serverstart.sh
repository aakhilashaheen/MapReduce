rm -rf intermediate_dir/*
rm -rf logging_dir/*
javac -cp ".:/usr/local/Thrift/*" ServerHandler.java -d . &&  javac -cp ".:/usr/local/Thrift/*" Client.java -d . && javac -cp ".:/usr/local/Thrift/*" WorkerHandler.java -d .
$TERM -e "java -cp \".:/usr/local/Thrift/*\" ServerHandler 9090 1"
$TERM -e "java -cp \".:/usr/local/Thrift/*\" WorkerHandler localhost 9090 9091 0.2"
$TERM -e "java -cp \".:/usr/local/Thrift/*\" WorkerHandler localhost 9090 9094 0.2"
java -cp ".:/usr/local/Thrift/*" Client localhost 9090 input_dir


