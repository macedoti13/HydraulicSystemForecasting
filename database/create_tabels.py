import os

from dotenv import load_dotenv
from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

load_dotenv()
connection_string = f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"

engine = create_engine(connection_string)

Base = declarative_base()


class BronzeElipseData(Base):
    __tablename__ = "elipse_bronze"
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, unique=True)
    flow_in_l_s = Column(Float, nullable=False)
    reservoir_level_percentage = Column(Float, nullable=False)
    pressure_mca = Column(Float, nullable=False)
    gmb_1_is_on = Column(Boolean, nullable=False)
    gmb_2_is_on = Column(Boolean, nullable=False)

    def __repr__(self):
        return (
            f"<ElipseBrozne(timestamp={self.timestamp}, flow_in_l_s={self.flow_in_l_s}, "
            f"reservoir_level_percentage={self.reservoir_level_percentage}, pressure_mca={self.pressure_mca}, "
            f"gmb_1_is_on={self.gmb_1_is_on}, gmb_2_is_on={self.gmb_2_is_on})>"
        )


# Define the climate_bronze table schema
class ClimateBronze(Base):
    __tablename__ = "climate_bronze"
    id = Column(Integer, primary_key=True, autoincrement=True)
    total_precip_mm = Column(Float)
    station_pressure_mb = Column(Float)
    max_pressure_last_hour_mb = Column(Float)
    min_pressure_last_hour_mb = Column(Float)
    max_temp_last_hour_c = Column(Float)
    min_temp_last_hour_c = Column(Float)
    max_dew_point_last_hour_c = Column(Float)
    min_dew_point_last_hour_c = Column(Float)
    max_humidity_last_hour_percentage = Column(Float)
    min_humidity_last_hour_percentage = Column(Float)
    relative_humidity_percentage = Column(Float)
    wind_direction_deg = Column(Float)
    max_wind_gust_m_s = Column(Float)
    wind_speed_m_s = Column(Float)

    def __repr__(self):
        return (
            f"<ClimateBronze(total_precip_mm={self.total_precip_mm}, station_pressure_mb={self.station_pressure_mb}, "
            f"max_pressure_last_hour_mb={self.max_pressure_last_hour_mb}, min_pressure_last_hour_mb={self.min_pressure_last_hour_mb}, "
            f"max_temp_last_hour_c={self.max_temp_last_hour_c}, min_temp_last_hour_c={self.min_temp_last_hour_c}, "
            f"max_dew_point_last_hour_c={self.max_dew_point_last_hour_c}, min_dew_point_last_hour_c={self.min_dew_point_last_hour_c}, "
            f"max_humidity_last_hour_percentage={self.max_humidity_last_hour_percentage}, min_humidity_last_hour_percentage={self.min_humidity_last_hour_percentage}, "
            f"relative_humidity_percentage={self.relative_humidity_percentage}, wind_direction_deg={self.wind_direction_deg}, "
            f"max_wind_gust_m_s={self.max_wind_gust_m_s}, wind_speed_m_s={self.wind_speed_m_s})>"
        )


# Define the silver_merge table schema
class SilverMerge(Base):
    __tablename__ = "silver_merge"
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, unique=True)
    flow_in_l_s = Column(Float, nullable=False)
    reservoir_level_percentage = Column(Float, nullable=False)
    pressure_mca = Column(Float, nullable=False)
    gmb_1_is_on = Column(Boolean, nullable=False)
    gmb_2_is_on = Column(Boolean, nullable=False)
    total_precip_mm = Column(Float)
    station_pressure_mb = Column(Float)
    max_pressure_last_hour_mb = Column(Float)
    min_pressure_last_hour_mb = Column(Float)
    max_temp_last_hour_c = Column(Float)
    min_temp_last_hour_c = Column(Float)
    max_dew_point_last_hour_c = Column(Float)
    min_dew_point_last_hour_c = Column(Float)
    max_humidity_last_hour_percentage = Column(Float)
    min_humidity_last_hour_percentage = Column(Float)
    relative_humidity_percentage = Column(Float)
    wind_direction_deg = Column(Float)
    max_wind_gust_m_s = Column(Float)
    wind_speed_m_s = Column(Float)

    def __repr__(self):
        return (
            f"<SilverMerge(timestamp={self.timestamp}, flow_in_l_s={self.flow_in_l_s}, reservoir_level_percentage={self.reservoir_level_percentage}, "
            f"pressure_mca={self.pressure_mca}, gmb_1_is_on={self.gmb_1_is_on}, gmb_2_is_on={self.gmb_2_is_on}, "
            f"total_precip_mm={self.total_precip_mm}, station_pressure_mb={self.station_pressure_mb}, max_pressure_last_hour_mb={self.max_pressure_last_hour_mb}, "
            f"min_pressure_last_hour_mb={self.min_pressure_last_hour_mb}, max_temp_last_hour_c={self.max_temp_last_hour_c}, min_temp_last_hour_c={self.min_temp_last_hour_c}, "
            f"max_dew_point_last_hour_c={self.max_dew_point_last_hour_c}, min_dew_point_last_hour_c={self.min_dew_point_last_hour_c}, "
            f"max_humidity_last_hour_percentage={self.max_humidity_last_hour_percentage}, min_humidity_last_hour_percentage={self.min_humidity_last_hour_percentage}, "
            f"relative_humidity_percentage={self.relative_humidity_percentage}, wind_direction_deg={self.wind_direction_deg}, "
            f"max_wind_gust_m_s={self.max_wind_gust_m_s}, wind_speed_m_s={self.wind_speed_m_s})>"
        )


# Create the tables
Base.metadata.create_all(engine)
