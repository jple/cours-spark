while true;
do
	# ecriture de csv dans la source1
	filename=$(shuf -i 0-9999999 -n 1)
	id=$(shuf -i 10000-99999 -n 1)
	echo "id, val1, val2" >> dump/source1/$filename.csv
	for numero in `seq 1 10`
		echo $id, $(shuf -i 0-100 -n 1), $(shuf -i 0-500 -n 1) >> dump/source1/$filename.csv
	done


	# ecriture de csv dans la source2
	filename=$(shuf -i 0-9999999 -n 1)
	id=$(shuf -i 10000-99999 -n 1)
	echo "id, val1, val2" >> dump/source2/$filename.csv
	for numero in `seq 1 10`
		echo $id, $(shuf -i 0-100 -n 1), $(shuf -i 0-500 -n 1) >> dump/source2/$filename.csv
	done

	sleep 1
done