import os
import pandas as pd
from sqlalchemy import create_engine
import watchdog
import logging

class PandasStateWatcher:
  def __init__(self, driver='sqlite', connstring=''):
    """A Class to keep track of the data from the file system handlers
    and to write to and from the DB

    Arguments:
      driver: DB Driver. Defaults to SQLite
      connstring: Connection string for the database
    """
    self.dataframes = dict()
    self.engine     = create_engine(f'{driver}:///{connstring}', echo=False)

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

  def update_dataframes(self, src_path):
    """Updates a dataframe and records the transaction in the database

    Arguments:
      src_path: The removed file path
    """
    self.add_dataframe(src_path)

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
  
