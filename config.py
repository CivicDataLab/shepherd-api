import os

from simple_config import Config

default_settings = {
    'tasks': {
        'skip_column': 'SkipColumn',
        'merge_columns': 'MergeColumns',
        'Ckan': 'CkanTOIDP'
    }
}
config_file = os.path.join(os.path.dirname(__file__), 'config.json')
settings = Config(config_file, defaults=default_settings)
