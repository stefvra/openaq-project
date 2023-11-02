import logging
import geopy
import polars as pl


logger = logging.Logger(__name__)
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(sh)


class coordinate_store:
    """Coordinate store defines an api for retreiving location coordinates.
    The locations that are already retreived are stored in a local store to
    avoid querying the location service too often.
    """

    # Schema to be used in the DataFrame
    location_schema = {"location": str, "latitude": pl.Float64, "longitude": pl.Float64}

    def __init__(self, geolocator, filename: str):
        """Initializer for class. Initializes the local coordinate store.

        Args:
            geolocator (_type_): _description_
            filename (str): _description_
        """

        self.filename = filename
        self.geolocator = geolocator
        self.df = self.init_file()

    
    def init_file(self) -> pl.DataFrame:
        """Initializes the local file that contains coordinate info.

        Returns:
            pl.DataFrame: The actual coordinates stored
        """

        try:
            # Try to read in existing file
            df = pl.read_parquet(self.filename)
        except FileNotFoundError:
            # If file does not exist, create one.
            logger.warning("no local location store found, generating...")
            df = pl.DataFrame(None, schema=self.location_schema)
            df.write_parquet(self.filename)
        return df
    
    
    
    def get(self, location_name: str) -> list:
        """Gets coordinate info. First queries local store, if not
        successful, query location service

        Args:
            location_name (str): location to get coordinates for

        Returns:
            list: latitude, longitode
        """
        
        # filter existing store to location
        df_filtered = self.df.filter(pl.col("location") == location_name)
        
        
        if len(df_filtered) == 0:
            # if location is not in store
            logger.warning(f"location {location_name} not found in local store, retreiving from geolocator...")
            # query location service
            location = self.geolocator.geocode(location_name)
            if location is None:
                # if location not in store, return None
                logger.warning(f"location {location_name} not found in geolocator, skipping...")
                return None
            latitude = location.latitude
            longitude = location.longitude
            # add location to local store and overwrite local store
            df_new = pl.DataFrame({"location": location_name, "latitude": latitude, "longitude": longitude}, schema=self.location_schema)
            self.df.extend(df_new)
            self.df.write_parquet(self.filename)
        else:
            logger.warning(f"location {location_name} found in local store...")
            latitude = df_filtered["latitude"].item()
            longitude = df_filtered["longitude"].item()
        
        return latitude, longitude
    


    def get_all(self) -> pl.DataFrame:
        """Returns all coordinates as a dataframe

        Returns:
            pl.DataFrame: all coordinates inthe store
        """
        return self.df