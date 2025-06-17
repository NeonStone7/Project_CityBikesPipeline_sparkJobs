from src.initial_processor import (
    initial_processing
    )
from src.aggregator import (
    create_agg_networks,
    create_bike_activity,
    create_agg_stations
)


definitions = {
    'initial_network_stations': {
        'transformer': initial_processing,
        'schema': 'initial_processed',
        'fields': {
            'network_id': 'string',
            'network_name': 'string',
            'network_latitude': 'double',
            'network_longitude': 'double',
            'network_city': 'string',
            'network_country': 'string',
            'network_href': 'string',
            'network_company': 'string',
            'network_gbfs_href': 'string',
            'station_id': 'string',
            'station_name': 'string',
            'station_latitude': 'double',
            'station_longitude': 'double',
            'station_timestamp': 'string',
            'station_free_bikes': 'int',
            'station_empty_slots': 'int',
            'processing_timestamp': 'timestamp',
            'year': 'int',
            'month': 'int',
            'day': 'int'
        }
    },

    'stations': {
        'transformer': create_agg_stations,
        'schema': 'aggregated',
        'fields': {
            'station_id':'string',
            'station_name': 'string',
            'station_latitude': 'double',
            'station_longitude': 'double',
            'station_timestamp': 'string',
            'processing_timestamp': 'timestamp',
            'year': 'int',
            'month': 'int',
            'day': 'int'
        }
    },

    'bike_activity': {
        'transformer': create_bike_activity,
        'schema': 'aggregated',
        'fields': {
            'network_id': 'string',
            'station_id': 'string',
            'event_day': 'int',
            'hour': 'string',
            'num_free_bikes': 'bigint',
            'num_empty_slots': 'bigint',
            'processing_timestamp': 'timestamp',
            'year': 'int',
            'month': 'int'
        }
    },

    'networks': {
        'transformer': create_agg_networks,
        'schema': 'aggregated',
        'fields': {
            'network_id': 'string',
            'network_name': 'string',
            'network_latitude': 'double',
            'network_longitude': 'double',
            'network_city': 'string', 
            'network_country': 'string',
            'network_href': 'string',
            'network_company': 'string',
            'network_gbfs_href': 'string',
            'processing_timestamp': 'timestamp',
            'year': 'int',
            'month': 'int', 
            'day': 'int'
        }
    }
}