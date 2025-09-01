#!/usr/bin/env python3
"""Clustering analysis for luxury beauty segmentation."""

import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import structlog
from sklearn.cluster import MiniBatchKMeans, KMeans
from sklearn.metrics import silhouette_score, davies_bouldin_score
# Visualization imports (optional)
# from sklearn.manifold import TSNE
# import matplotlib.pyplot as plt
# import seaborn as sns

logger = structlog.get_logger()

class ClusteringAnalyzer:
    """Clustering analysis for luxury beauty segmentation."""
    
    def __init__(self):
        """Initialize clustering analyzer."""
        self.feature_matrix = None
        self.products_df = None
        self.clustering_results = {}
        
    def load_data(self):
        """Load feature matrix and products data."""
        # Load feature matrix
        feature_path = "data/silver/feature_matrix.parquet"
        if Path(feature_path).exists():
            self.feature_matrix = pd.read_parquet(feature_path)
            logger.info(f"Loaded feature matrix with {len(self.feature_matrix)} products")
        else:
            logger.warning(f"Feature matrix not found: {feature_path}")
            return
        
        # Load products for interpretation
        products_path = "data/silver/sample_products.parquet"
        if Path(products_path).exists():
            self.products_df = pd.read_parquet(products_path)
            logger.info(f"Loaded products data with {len(self.products_df)} products")
        
        # Use feature matrix directly since it already contains product data
        if self.feature_matrix is not None:
            self.analysis_df = self.feature_matrix.copy()
            logger.info(f"Using feature matrix with {len(self.analysis_df)} products")
    
    def find_optimal_k(self, k_range: range = range(2, 5)) -> Dict:
        """Find optimal number of clusters using multiple metrics."""
        if self.feature_matrix is None:
            return {}
        
        # Prepare features (exclude product_id)
        features = self.feature_matrix.drop('product_id', axis=1)
        
        results = {
            'k_values': list(k_range),
            'silhouette_scores': [],
            'davies_bouldin_scores': [],
            'inertia': []
        }
        
        for k in k_range:
            logger.info(f"Testing k={k}")
            
            # Fit KMeans
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            cluster_labels = kmeans.fit_predict(features)
            
            # Calculate metrics
            silhouette = silhouette_score(features, cluster_labels)
            davies_bouldin = davies_bouldin_score(features, cluster_labels)
            inertia = kmeans.inertia_
            
            results['silhouette_scores'].append(silhouette)
            results['davies_bouldin_scores'].append(davies_bouldin)
            results['inertia'].append(inertia)
            
            logger.info(f"k={k}: Silhouette={silhouette:.3f}, DB={davies_bouldin:.3f}, Inertia={inertia:.0f}")
        
        # Find optimal k
        optimal_silhouette_k = k_range[np.argmax(results['silhouette_scores'])]
        optimal_db_k = k_range[np.argmin(results['davies_bouldin_scores'])]
        
        results['optimal_silhouette_k'] = optimal_silhouette_k
        results['optimal_db_k'] = optimal_db_k
        
        logger.info(f"Optimal k by Silhouette: {optimal_silhouette_k}")
        logger.info(f"Optimal k by Davies-Bouldin: {optimal_db_k}")
        
        return results
    
    def run_clustering(self, k: int = 6) -> Dict:
        """Run clustering with specified k."""
        if self.feature_matrix is None:
            return {}
        
        # Prepare features
        features = self.feature_matrix.drop('product_id', axis=1)
        
        # Fit MiniBatchKMeans
        kmeans = MiniBatchKMeans(n_clusters=k, random_state=42, batch_size=100)
        cluster_labels = kmeans.fit_predict(features)
        
        # Add cluster labels to analysis dataframe
        self.analysis_df['cluster'] = cluster_labels
        
        # Calculate cluster statistics
        cluster_stats = self.analysis_df.groupby('cluster').agg({
            'price_value': ['count', 'mean', 'std'],
            'refillable_flag': 'mean',
            'is_luxury': 'mean'
        }).round(3)
        
        # Flatten column names
        cluster_stats.columns = ['product_count', 'avg_price', 'price_std', 'refill_rate', 'luxury_rate']
        
        # Calculate feature importance per cluster
        feature_importance = self._calculate_feature_importance(features, cluster_labels)
        
        results = {
            'k': k,
            'cluster_labels': cluster_labels,
            'cluster_stats': cluster_stats,
            'feature_importance': feature_importance,
            'silhouette_score': silhouette_score(features, cluster_labels),
            'davies_bouldin_score': davies_bouldin_score(features, cluster_labels)
        }
        
        self.clustering_results = results
        logger.info(f"Clustering complete with k={k}, Silhouette={results['silhouette_score']:.3f}")
        
        return results
    
    def _calculate_feature_importance(self, features: pd.DataFrame, cluster_labels: np.ndarray) -> pd.DataFrame:
        """Calculate feature importance for each cluster."""
        importance_data = []
        
        for cluster_id in np.unique(cluster_labels):
            cluster_mask = cluster_labels == cluster_id
            cluster_features = features[cluster_mask]
            other_features = features[~cluster_mask]
            
            for feature in features.columns:
                cluster_mean = cluster_features[feature].mean()
                other_mean = other_features[feature].mean()
                importance = abs(cluster_mean - other_mean)
                
                importance_data.append({
                    'cluster': cluster_id,
                    'feature': feature,
                    'cluster_mean': cluster_mean,
                    'other_mean': other_mean,
                    'importance': importance
                })
        
        importance_df = pd.DataFrame(importance_data)
        return importance_df.sort_values(['cluster', 'importance'], ascending=[True, False])
    
    def generate_cluster_profiles(self) -> Dict:
        """Generate detailed cluster profiles for interpretation."""
        if not hasattr(self, 'analysis_df') or 'cluster' not in self.analysis_df.columns:
            logger.warning("No clustering results available")
            return {}
        
        profiles = {}
        
        for cluster_id in sorted(self.analysis_df['cluster'].unique()):
            cluster_data = self.analysis_df[self.analysis_df['cluster'] == cluster_id]
            
            # Basic statistics
            profile = {
                'cluster_id': int(cluster_id),
                'size': len(cluster_data),
                'avg_price': cluster_data['price_value'].mean(),
                'price_range': f"{cluster_data['price_value'].min():.0f}-{cluster_data['price_value'].max():.0f}",
                'refill_rate': cluster_data['refillable_flag'].mean(),
                'luxury_rate': cluster_data['is_luxury'].mean()
            }
            
            # Add brand info if available
            if 'brand' in cluster_data.columns:
                profile['top_brands'] = cluster_data['brand'].value_counts().head(3).to_dict()
            
            # Add product names if available
            if 'name' in cluster_data.columns:
                profile['top_products'] = cluster_data['name'].head(3).tolist()
            
            # Feature characteristics
            feature_cols = [col for col in cluster_data.columns if col not in ['product_id', 'brand', 'name', 'cluster']]
            numeric_cols = cluster_data[feature_cols].select_dtypes(include=[np.number]).columns
            
            profile['key_features'] = {}
            for col in numeric_cols:
                profile['key_features'][col] = cluster_data[col].mean()
            
            profiles[cluster_id] = profile
        
        return profiles
    
    def run_ablation_study(self) -> Dict:
        """Run ablation study (with vs without sustainability features)."""
        if self.feature_matrix is None:
            return {}
        
        # Full feature set
        full_features = self.feature_matrix.drop('product_id', axis=1)
        
        # Remove sustainability features
        sustainability_features = ['refillable_flag', 'refill_shelf_share', 'eco_badge_count']
        reduced_features = full_features.drop(sustainability_features, axis=1, errors='ignore')
        
        # Test both feature sets
        results = {}
        
        for k in [2, 3, 4]:  # Adjusted for small dataset
            logger.info(f"Testing ablation with k={k}")
            
            # Full features
            kmeans_full = MiniBatchKMeans(n_clusters=k, random_state=42)
            labels_full = kmeans_full.fit_predict(full_features)
            
            # Reduced features
            kmeans_reduced = MiniBatchKMeans(n_clusters=k, random_state=42)
            labels_reduced = kmeans_reduced.fit_predict(reduced_features)
            
            results[f'k_{k}'] = {
                'full_features': {
                    'silhouette': silhouette_score(full_features, labels_full),
                    'davies_bouldin': davies_bouldin_score(full_features, labels_full)
                },
                'reduced_features': {
                    'silhouette': silhouette_score(reduced_features, labels_reduced),
                    'davies_bouldin': davies_bouldin_score(reduced_features, labels_reduced)
                }
            }
        
        logger.info("Ablation study complete")
        return results
    
    def save_results(self, output_dir: str = "data/silver"):
        """Save clustering results."""
        if not self.clustering_results:
            logger.warning("No clustering results to save")
            return
        
        # Save cluster assignments
        if hasattr(self, 'analysis_df'):
            self.analysis_df.to_parquet(f"{output_dir}/clustered_products.parquet", index=False)
        
        # Save cluster statistics
        if 'cluster_stats' in self.clustering_results:
            self.clustering_results['cluster_stats'].to_csv(f"{output_dir}/cluster_statistics.csv")
        
        # Save feature importance
        if 'feature_importance' in self.clustering_results:
            self.clustering_results['feature_importance'].to_csv(f"{output_dir}/feature_importance.csv", index=False)
        
        # Save cluster profiles
        profiles = self.generate_cluster_profiles()
        # Convert numpy types to native Python types for JSON serialization
        profiles_serializable = {}
        for k, v in profiles.items():
            profiles_serializable[str(k)] = v
        with open(f"{output_dir}/cluster_profiles.json", "w") as f:
            json.dump(profiles_serializable, f, indent=2)
        
        # Save ablation results
        ablation_results = self.run_ablation_study()
        with open(f"{output_dir}/ablation_results.json", "w") as f:
            json.dump(ablation_results, f, indent=2)
        
        logger.info(f"Clustering results saved to {output_dir}")

def main():
    """Main execution function."""
    analyzer = ClusteringAnalyzer()
    analyzer.load_data()
    
    if analyzer.feature_matrix is None:
        logger.warning("No feature matrix available for clustering")
        return
    
    print("\n" + "="*60)
    print("CLUSTERING ANALYSIS")
    print("="*60)
    
    # Find optimal k
    print("🔍 Finding optimal number of clusters...")
    optimal_k_results = analyzer.find_optimal_k()
    
    if optimal_k_results:
        print(f"✅ Optimal k by Silhouette: {optimal_k_results.get('optimal_silhouette_k', 'N/A')}")
        print(f"✅ Optimal k by Davies-Bouldin: {optimal_k_results.get('optimal_db_k', 'N/A')}")
    
    # Run clustering with recommended k
    recommended_k = optimal_k_results.get('optimal_silhouette_k', 6)
    print(f"\n🎯 Running clustering with k={recommended_k}...")
    
    clustering_results = analyzer.run_clustering(k=recommended_k)
    
    if clustering_results:
        print(f"✅ Clustering complete!")
        print(f"   Silhouette Score: {clustering_results['silhouette_score']:.3f}")
        print(f"   Davies-Bouldin Score: {clustering_results['davies_bouldin_score']:.3f}")
        
        # Show cluster statistics
        print(f"\n📊 Cluster Statistics:")
        print(clustering_results['cluster_stats'])
        
        # Save results
        analyzer.save_results()
        
        print(f"\n📁 Results saved to data/silver/")
        print("="*60)
    else:
        print("❌ Clustering failed")

if __name__ == "__main__":
    main()
