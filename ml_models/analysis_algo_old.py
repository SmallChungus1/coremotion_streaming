#OLD ML model training pipeline for walking vs Driving mode prediction

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
import pandas as pd
import json
import os

class TransportModeClassifier:
    def __init__(self, app_name="UserModeClassification"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        
    def load_data(self, path):
        if not path.startswith("file://") and not path.startswith("hdfs://"):
            path = "file://" + os.path.abspath(path)
            
        print(f"Loading data from {path}")
        df = self.spark.read.csv(path, header=True, inferSchema=True)
        return df

    def preprocess(self, df):
        df = df.dropna()
        
        indexer = StringIndexer(inputCol="USER_MODE", outputCol="label")
        indexer_model = indexer.fit(df)
        df_indexed = indexer_model.transform(df)
        
        self.labels = indexer_model.labels
        print(f"Labels mapping: {list(enumerate(self.labels))}")
        
        exclude_cols = ["STREAM_KEY", "USER_MODE", "WINDOW_START", "WINDOW_END", "label"]
        feature_cols = [c for c in df.columns if c not in exclude_cols and c != "label"]
        
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df_assembled = assembler.transform(df_indexed)
        
        return df_assembled, feature_cols

    def train_evaluate_all_models(self, df, feature_cols, models_output_dir):
        train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
        
        models = {
            "RandomForest": RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=20, seed=42),
            "LogisticRegression": LogisticRegression(labelCol="label", featuresCol="features", maxIter=10),
            "DecisionTree": DecisionTreeClassifier(labelCol="label", featuresCol="features", seed=42)
        }
        
        best_model_name = None
        best_accuracy = -1
        all_models_results = {}
        trained_models = {}
        
        rf_feature_importance = []

        print("\nTraining and Evaluating Models...")
        
        for name, algo in models.items():
            print(f"\n--- {name} ---")
            model = algo.fit(train_data)
            predictions = model.transform(test_data)
            
            evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
            evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
            evaluator_prec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
            evaluator_rec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")
            
            accuracy = evaluator_acc.evaluate(predictions)
            f1 = evaluator_f1.evaluate(predictions)
            precision = evaluator_prec.evaluate(predictions)
            recall = evaluator_rec.evaluate(predictions)
            
            print(f"Accuracy: {accuracy:.4f}")
            print(f"F1 Score: {f1:.4f}")
            
            predictionAndLabels = predictions.select("prediction", "label").rdd.map(lambda x: (float(x[0]), float(x[1])))
            metrics = MulticlassMetrics(predictionAndLabels)
            confusion_matrix = metrics.confusionMatrix().toArray()
            
            feature_importance = []
            if hasattr(model, 'featureImportances'):
                importances = model.featureImportances
                for i, feature in enumerate(feature_cols):
                    feature_importance.append({"feature": feature, "importance": float(importances[i])})
                feature_importance.sort(key=lambda x: x["importance"], reverse=True)
                
                if name == "RandomForest":
                    rf_feature_importance = feature_importance
            
            all_models_results[name] = {
                "model_name": name,
                "accuracy": accuracy,
                "f1_score": f1,
                "precision": precision,
                "recall": recall,
                "confusion_matrix": confusion_matrix.tolist(),
                "labels": self.labels,
                "feature_importance": feature_importance
            }
            
            model_path = os.path.join(models_output_dir, f"{name}_model")
            model.write().overwrite().save(model_path)
            print(f"Model saved to {model_path}")
            
            trained_models[name] = model

            if accuracy > best_accuracy:
                best_accuracy = accuracy
                best_model_name = name
        
        summary_results = {
            "best_model": best_model_name,
            "best_accuracy": best_accuracy,
            "all_models": all_models_results,
            "feature_importance": rf_feature_importance,
            "labels": self.labels
        }
        
        print(f"\nBest Model based on Accuracy: {best_model_name} with Accuracy: {best_accuracy:.4f}")
        
        return summary_results, trained_models

    def run(self, data_path, output_dir, models_dir=None):
        df = self.load_data(data_path)
        df_processed, feature_cols = self.preprocess(df)
        
        if models_dir is None:
            models_dir = os.path.join(output_dir, "models")
        if not os.path.exists(models_dir):
            os.makedirs(models_dir)
        
        results, trained_models = self.train_evaluate_all_models(df_processed, feature_cols, models_dir)
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        if results['feature_importance']:
            pd.DataFrame(results['feature_importance']).to_csv(os.path.join(output_dir, "feature_importance.csv"), index=False)
        
        with open(os.path.join(output_dir, "all_models_results.json"), "w") as f:
            json.dump(results, f, indent=4)
            
        best_model_results = results['all_models'][results['best_model']]
        best_model_results['best_model'] = results['best_model']
        best_model_results['feature_importance'] = results['feature_importance']
        with open(os.path.join(output_dir, "model_results.json"), "w") as f:
            json.dump(best_model_results, f, indent=4)
            
        print(f"Results saved to {output_dir}")
        print(f"Models saved to {models_dir}")
        
        self.spark.stop()
        
        return results

if __name__ == "__main__":
    classifier = TransportModeClassifier()
    classifier.run(
        data_path="/root/coremotion_streaming-main/ios_stream_helper_2025-11-27-1426.csv",
        output_dir="/root/coremotion_streaming-main/model_output"
    )
