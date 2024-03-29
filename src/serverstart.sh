mkdir -p intermediate_dir
mkdir -p logging_dir
mkdir -p output_dir
rm -rf intermediate_dir/*
rm -rf *.class
javac -cp ".:/usr/local/Thrift/*" ServerHandler.java -d . &&  javac -cp ".:/usr/local/Thrift/*" Client.java -d . && javac -cp ".:/usr/local/Thrift/*" WorkerHandler.java -d .
gnome-terminal -e "java -cp \".:/usr/local/Thrift/*\" ServerHandler 9090 1"
gnome-terminal -e "java -cp \".:/usr/local/Thrift/*\" WorkerHandler localhost 9090 9091 0.2"
gnome-terminal -e "java -cp \".:/usr/local/Thrift/*\" WorkerHandler localhost 9090 9092 0.2"
gnome-terminal -e "java -cp \".:/usr/local/Thrift/*\" WorkerHandler localhost 9090 9093 0.2"
gnome-terminal -e "java -cp \".:/usr/local/Thrift/*\" WorkerHandler localhost 9090 9094 0.2"
sleep 3s
java -cp ".:/usr/local/Thrift/*" Client localhost 9090 input_dir


