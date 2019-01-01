while true;
do
	# ecriture de csv dans la source1
	filename=$(shuf -i 0-9999999 -n 1)
	echo "id,val1,val2" >> dump/source1/$filename.csv
	
	for _ in `seq 1 5`
	do
		idf=$(shuf -i 10000-99999 -n 1)
		for _ in `seq 1 10`
		do
			echo $idf,$(shuf -i 0-100 -n 1),$(shuf -i 0-500 -n 1) >> dump/source1/$filename.csv
		done
	done

	# ecriture de csv dans la source2
	filename=$(shuf -i 0-9999999 -n 1)
	echo "id,val1,val2" >> dump/source2/$filename.csv
	
	for _ in `seq 1 5`
	do
		idf=$(shuf -i 10000-99999 -n 1)
		for _ in `seq 1 10`
		do
			echo $idf,$(shuf -i 0-100 -n 1),$(shuf -i 0-500 -n 1) >> dump/source2/$filename.csv
		done
	done

	sleep 1
done