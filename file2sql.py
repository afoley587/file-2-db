import argparse
import sys
import time
import logging
from watchdog.observers import Observer
from pandas_fs_event_handler import PandasFileSystemEventHander

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

if __name__ == "__main__":
  main()    