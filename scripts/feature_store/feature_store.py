import json
import pandas as pd
import os

FEATURE_DATA_PATH = "data_lake/transformed_data"


class FeatureStore:
    def __init__(self, registry_path="config/feature_registry.json"):
        with open(registry_path, "r") as f:
            self.registry = json.load(f)

    def get_feature(self, feature_name, entity_ids):
        """
        Retrieve a specific feature for given entity IDs
        """
        if feature_name not in self.registry:
            raise ValueError(f"Feature '{feature_name}' not found in registry")

        feature_meta = self.registry[feature_name]

        source_table = feature_meta["source_table"]
        version = feature_meta["version"]
        entity_key = feature_meta["entity_key"]

        file_path = (
            f"{FEATURE_DATA_PATH}/{source_table}/"
            f"{source_table}_v{version}.csv"
        )

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Feature data not found: {file_path}")

        df = pd.read_csv(file_path)

        return df[df[entity_key].isin(entity_ids)]

    def list_features(self):
        """List all registered features"""
        return list(self.registry.keys())
