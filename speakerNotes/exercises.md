## Step 1. Meet the beast

- Hello world in spark shell

## Step 2.
**git checkout step1**

- Show and explain project
- Hello with cache
	- add dummy transformation with sleep or logger to see it does not hapens twice
- Show difference between trasnformation an action
- Show how spark optimizes graph (step2b)

## Step 3.
**git checkout step2**

- Extract logic to services in the simplest way
- Show a test

## Step 4.
**git checkout step3**

- Extract logic outside and show serialization problems (step4a)

**git checkout step4a**

- Find a solution

## Step 5. Jam session!!
**git checkout step4b**

1. Simple RDD
	- Filter = Count all words in files with more than X characters
	- Maps = Convert all words in capitals
	- Reduces = Average of characters per word
	- Unions = Count all words in two files
	- Intersection = Words which are in both files

### ¡¡Be careful with actions (collect in driver is dangerous)!!

2. Work with RDD KeyValue
	- Count word occurrences
	- Number of words by length
	- Count word occurrences with two files as input (only for words which are in both files)

## Step 6.
- Talk about new RDD inputs (newHadoopApi)
	- Add mask with matching files
	- Add mask with no matching files

## Step 7.
- Work with structured data
	- Parse csv file
	- Parse json files and work with them

## Step 8.
- Show spark sql
	- load from json
	- load from csv with header
	- load from csv without header



