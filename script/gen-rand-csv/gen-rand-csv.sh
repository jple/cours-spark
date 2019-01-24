while true;
do
	# ecriture de csv dans la source1
	filename=$(shuf -i 0-9999999 -n 1)
	echo "id,date,val1,val2" >> dump/gen-rand-csv/source1/$filename.csv
	
	for _ in `seq 1 5`
	do
		idf=$(shuf -i 10000-99999 -n 1)
		init_date=$(date -d '2018-02-03' '+%Y-%m-%d')
		rand=$(shuf -i 0-3 -n 1)
		rand_date=$(date -d "$init_date $rand days"  '+%Y-%m-%d')
		for k in `seq 1 10`
		do
			next_date=$(date -d "$rand_date $k days" "+%Y-%m-%d")
			echo $idf,$next_date,$(shuf -i 0-100 -n 1),$(shuf -i 0-500 -n 1) >> dump/gen-rand-csv/source1/$filename.csv
		done
	done


	# ecriture de csv dans la source2
	filename=$(shuf -i 0-9999999 -n 1)
	echo "id,val1,val2" >> dump/gen-rand-csv/source2/$filename.csv
	
	for _ in `seq 1 5`
	do
		idf=$(shuf -i 10000-99999 -n 1
		init_date=$(date -d '2018-02-03' '+%Y-%m-%d')
		rand=$(shuf -i 0-3 -n 1)
		rand_date=$(date -d "$init_date $rand days"  '+%Y-%m-%d')
		for k in `seq 1 10`
		do
			next_date=$(date -d "$rand_date $k days" "+%Y-%m-%d")
			echo $idf,$next_date,$(shuf -i 0-100 -n 1),$(shuf -i 0-500 -n 1) >> dump/gen-rand-csv/source2/$filename.csv
		done
	done



	sleep 1
done
