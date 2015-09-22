## Step 1. Meet the beast
	- Hello world in spark shell

## Step 2.
	- Show and explain project
	- Hello with cache
		- add dummy transformation with sleep or logger to see it does not hapens twice
	- Show difference between trasnformation an action

## Step 3
	- Extract logic to services in the simplest way
	- Show a test

## Step 4
	Extract logic outside and show serialization problems (step4a)
	Find a solution step4b

## Step 5 Jam session!!
	- Simple RDD
		- Filter = Count all words in files with more than X characters
		- Maps = Convert all words in capitals
		- Reduces = Average of characters per word
		- Unions = Count all words in two files
		- Intersection = Words wich are in both files

		¡¡Be careful with actions (collect in driver is dangerous)!!

	- Work with RDD KeyValue
		- Count word ocurrences
		- Number of words by length
		- Count word ocurrences with two files as input (only for words wich are in both files)

## Step 6
	- Work with structured data
		- Parse csv file
		- Parse json files and work with them

## Step 7
	- Show spark sql
		- load from json
		- load from csv with header
		- load from csv without header

## Step 8
	- Talk about new RDD inputs (newHadoopApi)
		- Add mask with matching files
		- Add mask with no matching files



