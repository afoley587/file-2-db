# Automatically Sync Files To A DB

So, my real motivation for this project was to learn more about
the Python Watchdog library for some of my professional company's
datacenter needs. The TLDR; we were looking for a configurable
and flexible way to react to file changes.

Now, there is the linux filesystem watcher, LSyncd, and some
other great tools, but python has such a rich ecosystem of
utilities which we could plug in to. For example, SDKs
for datadog or another monitoring utility, or we could've
wrapped it with a server framework like FastAPI, or a million of
other options. So, I decided to do some homework on the Watchdog packages
and see what was possible with python and its great!

Today, what were going to build is a python utility which will sync
file contents to a database. Were those contents to change, the database
would be updated to reflect the changes. We will also assume we only want to
handle CSV files, however, this could be expanded to more types!

I'm going to break this post down into the following sections:

* The Code Part I: The main loop
* The Code Part II: The models
* Running and the fun stuff

# The Code Part I: The main loop

The main event loop is nice, easy, and straight-forward. From a 
high-level perspective, we are going to:

1. Parse some command-line options from our users with `argparse`
2. Instantiate an event handler to do things when our filesystem is modified
3. Instantiate an observer who will watch the filesystem and call the event handler
4. Sit and wait for the observer to do its thing

Let's start with #1:

```python
def parse_args():
  """Parses the comand line options from the user
  """
  # Instantiate Parser
  parser = argparse.ArgumentParser(
    prog = 'file2sql',
    description = 'A program to monitor a file systems CSV files and convert them to SQL',
    epilog = 'Example: file2sql --directory ./test-dr --connstring ./test.db --output-format=sqlite')
  
  # Add Our Args
  parser.add_argument(
    '-d', '--directory', 
    help="Input directory to monitor.", required=True)
  parser.add_argument(
    '-c', '--connstring', 
    help='Output DB Connstring. If empty, an in memory DB will be used.', 
    required=False, default=':memory:')
  parser.add_argument(
    '-f', '--output-format', help='Output format. SQLite DB is supported.', 
    choices=['sqlite'], required=False, default='sqlite')
  
  # Parse the ones passed by the user and return
  opts = parser.parse_args()
  return opts
```

This is our helper function to parse the user's command line arguments. We have
an argument for a directory to watch, a database connection string, and a database
format. At this time, I only support sqlite! Finally, we just parse those out and return
them to the function caller so they can handle them however it wants.

Next, we move on to #2, 3, and 4. I have marked in the comments where those occur!

```python
def main():
  """Main python loop
  """

  # Parse options and set up logger
  # #1
  opts = parse_args()
  logging.basicConfig(level=logging.DEBUG,
                      format='%(asctime)s - %(message)s',
                      datefmt='%Y-%m-%d %H:%M:%S')

  # Set up our custom file system handler
  # #2
  event_handler = PandasFileSystemEventHander(
    driver=opts.output_format, 
    connstring=opts.connstring)

  # Set up the watch dog observer to watch our input path
  # and if changes occur, dispatch our event handler
  # #3
  observer = Observer()
  observer.schedule(event_handler, opts.directory, recursive=True)
  observer.start()

  # #4
  try:
    while True:
      time.sleep(1)
  except KeyboardInterrupt:
    observer.stop()
  observer.join()
```

So, we first see that we create a new event handler, `PandasFileSystemEventHander`.
When a file changes (or is removed or renamed), this is the handler that's going
to be called.

Next, we create a filesystem observer. Think of this as the object that is constantly
watching a directory. We then schedule it. This tells the observer that,
when it sees something change in the given directory, its going to call the 
`PandasFileSystemEventHander` we just instantiated who will do most of our business logic.

Finally, we enter our endless `while` loop and wait for a CTRL+C call from the user.

That's really it! Now we can get into the more nitty-gritty with the models we created.

# The Code Part II: The models
This part is slightly more difficult, but I think we can work through it together.
We created two classes to handle the events and then store them in a DB of our choosing.
We already saw the `PandasFileSystemEventHander` previously, but it holds another class
to do the database writing and dataframe maintenance, `PandasStateWatcher`. Note that we
don't necessarily use the dataframes except to create our SQL Queries. The pandas 
dataframes could have been omitted and we could have just stored the entire document
(or a shasum of it) in the database.

Let's get started with the `PandasFileSystemEventHander`:

```python
class PandasFileSystemEventHander(watchdog.events.FileSystemEventHandler):
  
  def __init__(self, driver, connstring):
    super().__init__()
    if not hasattr(PandasFileSystemEventHander, 'state_watcher'):
      PandasFileSystemEventHander.state_watcher = PandasStateWatcher(driver=driver, connstring=connstring)

  def dispatch(self, event):
    """Handles the invocation from the Watchdog.Observer

    Arguments:
      event: The filesystem event that occured
    """

    # Ignore directories and any non-CSV files
    if (event.is_directory) or (not (event.src_path.endswith('.csv'))):
      logging.debug(f"Ignoring {event.src_path}")
      return
    super().dispatch(event)

  def on_created(self, event):
    """Monitors when files are created

    Arguments:
      event: The filesystem event that occured
    """
    logging.debug(f"Created Event {event.src_path}")
    PandasFileSystemEventHander.state_watcher.add_dataframe(event.src_path)

  def on_deleted(self, event):
    """Monitors when files are deleted

    Arguments:
      event: The filesystem event that occured
    """
    logging.debug(f"Deleted Event {event.src_path}")
    PandasFileSystemEventHander.state_watcher.remove_dataframe(event.src_path)

  def on_modified(self, event):
    """Monitors when files are modified

    Arguments:
      event: The filesystem event that occured
    """
    logging.debug(f"Modified Event {event.src_path}")
    PandasFileSystemEventHander.state_watcher.update_dataframes(event.src_path)
  
  def on_moved(self, event):
    """Monitors when files are moved or renamed

    Arguments:
      event: The filesystem event that occured
    """
    logging.debug(f"Moved Event {event.src_path}")
    PandasFileSystemEventHander.state_watcher.add_dataframe(event.dest_path)
    PandasFileSystemEventHander.state_watcher.remove_dataframe(event.src_path)
```

The first thing you'll notice is that the `PandasFileSystemEventHander` inherits from the 
`watchdog.events.FileSystemEventHandler` base class. This is required
to use this as an event handler for the Watchdog Observer. We also instantiate a static
`state_watcher` which will be used to ingest the CSVs as dataframes and then record them
to a database.

Whenever the 
scheduler notices a change, it is going to call the `dispatch` method of its event handler.
As you'll see, ours is just being used to filter out any directories or any files that
don't end in the `.csv` extension! After that, it will follow the default dispatching.
The default dispatching looks like this:

* Was a file created? If so, call `on_created`
* Was a file deleted? If so, call `on_deleted`
* Was a file modified? If so, call `on_modified`
* Was a file moved? If so, call `on_moved`

It's now important to bring up our final model, the `PandasStateWatcher`, because the
`PandasFileSystemEventHander` uses it heavily. Let's go step-by-step with the methods
listed above.

`on_created` will call the `add_dataframe` method:

```python
  def add_dataframe(self, src_path):
    """Adds a dataframe and records the transaction in the database

    Arguments:
      src_path: The added file path
    """
    try:
      df = pd.read_csv(src_path)
    except FileNotFoundError:
      # Files deleted with modified buffers
      return
    except pd.errors.EmptyDataError:
      # New file added
      return 
    
    # Save the dataframe in our map to record changes
    # and output to 
    self.dataframes[src_path] = df
    self.__to_sql(src_path)
```

This function loads the dataframe using pandas, and then records it to a sql database. More
on the `__to_sql` method at the end of this section!

`on_deleted` calls the `remove_dataframe` method:

```python
  def remove_dataframe(self, src_path):
    """Removes a dataframe and records the transaction in the database

    Arguments:
      src_path: The removed file path
    """

    # Delete the key from our map
    try:
      del self.dataframes[src_path]
    except KeyError:
      return
    
    # drop the SQL Table
    self.__to_sql(src_path, operation = "drop")
```

This function just removes the dataframe from our internals and from the database.

`on_modified` calls the `update_dataframe` method:

```python
  def update_dataframes(self, src_path):
    """Updates a dataframe and records the transaction in the database

    Arguments:
      src_path: The removed file path
    """
    self.add_dataframe(src_path)
```

Essentially, our `add_dataframe` also handles updates. The only reason for breaking the two
up is for clarity to the programmer.

`on_moved` calls `add_dataframe` and then `remove_dataframe` to essentially move the internal
dataframe and sql objects to a new location in memory.

Finally, the `__to_sql` method is shown below for completeness:

```python
  def __to_sql(self, src_path, operation = "replace"):
    """Records a transaction for a file

    Arguments:
      src_path: The removed/added/updated file path
      operation: The SQL Operation to perform [replace or drop]
    """

    # name of the table is derived from the filename
    table_name = src_path.split(os.path.sep)[-1].split('.')[0]
    if (operation == "replace"):
      # Update the DB table
      self.dataframes[src_path].to_sql(table_name, con=self.engine, if_exists='replace')
      logging.debug(self.engine.execute(f"SELECT * FROM {table_name}").fetchall())
    elif (operation == "drop"):
      # Drop the DB Table
      self.engine.execute(f"DROP TABLE {table_name}")
    else:
      logging.error("Unrecognized")
```

The `__to_sql` method derives a table name from the given filename. If this is an update
(`replace`), then we just update the database record. If its a `drop`, then we can just drop
the table.

We are now feature complete! Let's try to run it.

# Running
You can run with the below script and simulate some file access:

```shell
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
```

You should see an output similar to:
```shell
file-2-db % ./run.sh 
Wait for startup...
0|l1|l2|l3
0|l1|l2|l3
1|l4|l5|l6
Error: in prepare, no such table: test1 (1)
file-2-db % 
```

And that's all there is to it! You've just built your file system monitoring utility!

# References
All code is hosted [here](https://github.com/afoley587/file-2-db) on my public github repo!