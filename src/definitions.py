from src.initial_processor import initial_processing


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
            'station_timestamp': 'timestamp',
            'station_free_bikes': 'int',
            'station_empty_slots': 'int',
            'processing_timestamp': 'timestamp',
            'year': 'int',
            'month': 'int',
            'day': 'int'
        }
    }
}