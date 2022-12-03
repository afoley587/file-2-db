#!/bin/bash

TEST_DIR=./test-dir
TEST_DB=./test.db

rm $TEST_DB
rm -rf $TEST_DIR
mkdir $TEST_DIR

poetry run python file2sql.py -d $TEST_DIR -c $TEST_DB &>/dev/null &
echo "Wait for startup..."
sleep 5
PID=$!

# Inserting a record into the CSV
echo -e "header1,header2,header2\nl1,l2,l3" > $TEST_DIR/test1.csv
sleep 2
# Proving that the SQL Record was reflected in the DB
# Should see
# 0|l1|l2|l3
sqlite3 $TEST_DB "SELECT * FROM test1"

# Inserting another record into the CSV
echo -e "l4,l5,l6" >> $TEST_DIR/test1.csv
sleep 2
# Proving that the SQL Record was reflected in the DB
# Should see 
# 0|l1|l2|l3
# 1|l4|l5|l6
sqlite3 $TEST_DB "SELECT * FROM test1"

# Removing the CSV File
rm $TEST_DIR/test1.csv
sleep 2
# Proving that the SQL Table was dropped in the DB
# Should see
# Error: in prepare, no such table: test1 (1)
sqlite3 $TEST_DB "SELECT * FROM test1"

kill $PID