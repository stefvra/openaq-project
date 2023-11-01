import logging
import geopy
import polars as pl


logger = logging.Logger(__name__)
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(sh)


class coordinate_store:
    """_summary_

    Returns:
        _type_: _description_
    """

    location_schema = {"location": str, "latitude": pl.Float64, "longitude": pl.Float64}

    def __init__(self, geolocator, filename):
        """_summary_

        Args:
            geolocator (_type_): _description_
            filename (_type_): _description_
        """
        self.filename = filename
        self.geolocator = geolocator
        self.df = self.init_file()

    
    def init_file(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        try:
            df = pl.read_parquet(self.filename)
        except FileNotFoundError:
            logger.warning("no local location store found, generating...")
            df = pl.DataFrame(None, schema=self.location_schema)
            df.write_parquet(self.filename)
        return df
    
    
    
    def get(self, location_name):
        """_summary_

        Args:
            location_name (_type_): _description_

        Returns:
            _type_: _description_
        """
        
        df_filtered = self.df.filter(pl.col("location") == location_name)
        
        if len(df_filtered) == 0:
            logger.warning(f"location {location_name} not found in local store, retreiving from geolocator...")
            location = self.geolocator.geocode(location_name)
            if location is None:
                logger.warning(f"location {location_name} not found in geolocator, skipping...")
                return None
            latitude = location.latitude
            longitude = location.longitude
            df_new = pl.DataFrame({"location": location_name, "latitude": latitude, "longitude": longitude}, schema=self.location_schema)
            self.df.extend(df_new)
            self.df.write_parquet(self.filename)
        else:
            logger.warning(f"location {location_name} found in local store...")
            latitude = df_filtered["latitude"].item()
            longitude = df_filtered["longitude"].item()
        
        return latitude, longitude
    


    def get_all(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        return self.df