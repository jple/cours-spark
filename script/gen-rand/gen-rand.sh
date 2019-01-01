while true;
do
rand_int=$RANDOM

#echo $rand_ind > dump/test.txt;
#hdfs dfs -rm dump/test.txt
#hdfs dfs -put -f dump/test.txt dump/test.txt

echo $rand_int > dump/$rand_int.txt
hdfs dfs -put dump/$rand_int.txt dump/$rand_int.txt
sleep 1;

done;
