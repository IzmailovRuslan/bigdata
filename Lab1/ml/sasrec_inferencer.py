import lightning as L
from torch.utils.data import DataLoader
import torch

from replay.data import (
    FeatureHint,
    FeatureInfo,
    FeatureSchema,
    FeatureSource,
    FeatureType,
    Dataset,
)

from replay.data.nn import (
    SequenceTokenizer,
    TensorFeatureSource,
    TensorSchema,
    TensorFeatureInfo
)
from replay.models.nn.sequential import SasRec
from replay.models.nn.sequential.sasrec import SasRecPredictionDataset
from rs_datasets import MovieLens

import pandas as pd


class SasRecInferencer():
    def __init__(self):
        self._tensor_schema = None
        self._tokenizer = None 
        self._prepare_schema_and_tokenizer()

        self._model = SasRec.load_from_checkpoint("ml/epoch=95-step=1152.ckpt", map_location=torch.device('cpu')).eval()

    def _prepare_schema_and_tokenizer(self):
        movielens = MovieLens("1m")
        interactions = movielens.ratings
        self.user_features = movielens.users
        self.item_features = movielens.items
        interactions["timestamp"] = interactions["timestamp"].astype("int64")
        interactions = interactions.sort_values(by="timestamp")
        interactions["timestamp"] = interactions.groupby("user_id").cumcount()

        sample_dataset = Dataset(
            feature_schema=self.prepare_feature_schema(is_ground_truth=False),
            interactions=interactions,
            query_features=self.user_features,
            item_features=self.item_features,
            check_consistency=True,
            categorical_encoded=False,
        )

        self._prepare_tensor_schema(sample_dataset)
        self._prepare_tokenizer(sample_dataset)


    def _prepare_tensor_schema(self, dataset):
        if self._tensor_schema is None:
            self._tensor_schema = TensorSchema(
                TensorFeatureInfo(
                    name="item_id_seq",
                    is_seq=True,
                    feature_type=FeatureType.CATEGORICAL,
                    feature_sources=[TensorFeatureSource(FeatureSource.INTERACTIONS, dataset.feature_schema.item_id_column)],
                    feature_hint=FeatureHint.ITEM_ID,
                )
            )
    
    def _prepare_tokenizer(self, dataset):
        if self._tokenizer is None:
            self._tokenizer = SequenceTokenizer(self._tensor_schema, allow_collect_to_master=True)
            self._tokenizer.fit(dataset)

    def prepare_feature_schema(self, is_ground_truth: bool) -> FeatureSchema:
        base_features = FeatureSchema(
            [
                FeatureInfo(
                    column="user_id",
                    feature_hint=FeatureHint.QUERY_ID,
                    feature_type=FeatureType.CATEGORICAL,
                ),
                FeatureInfo(
                    column="item_id",
                    feature_hint=FeatureHint.ITEM_ID,
                    feature_type=FeatureType.CATEGORICAL,
                ),
            ]
        )
        if is_ground_truth:
            return base_features

        all_features = base_features + FeatureSchema(
            [
                FeatureInfo(
                    column="timestamp",
                    feature_type=FeatureType.NUMERICAL,
                    feature_hint=FeatureHint.TIMESTAMP,
                ),
            ]
        )
        return all_features

    def create_sequential_dataset(self, interactions):
        dataset = Dataset(
            feature_schema=self.prepare_feature_schema(is_ground_truth=False),
            interactions=interactions,
            query_features=self.user_features,
            item_features=self.item_features,
            check_consistency=True,
            categorical_encoded=False,
        )
        
        sequential_dataset = self._tokenizer.transform(dataset)
        return sequential_dataset

    def predict(self, interactions):
        dataset = self.create_sequential_dataset(interactions)
        dataloader = DataLoader(
            dataset=SasRecPredictionDataset(
                dataset,
                max_sequence_length=100,
            ),
            batch_size=1,
            num_workers=4,
            pin_memory=True,
        )
        all_recommendations = []
        for batch in dataloader:
            scores = self._model.predict_step(batch, 0)
            recommendations = torch.topk(scores, k=10).indices
            all_recommendations.append(self.item_features.iloc[recommendations[0].detach().numpy()])
        return pd.concat(all_recommendations)