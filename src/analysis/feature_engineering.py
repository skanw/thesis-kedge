#!/usr/bin/env python3
"""Feature engineering for clustering analysis."""

import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import structlog
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.feature_extraction.text import TfidfVectorizer
import re

logger = structlog.get_logger()

class FeatureEngineer:
    """Feature engineering for luxury beauty clustering."""
    
    def __init__(self):
        """Initialize feature engineer."""
        self.scaler = StandardScaler()
        self.text_vectorizer = TfidfVectorizer(
            max_features=100,
            stop_words='english',
            ngram_range=(1, 2),
            min_df=2
        )
        
    def load_data(self):
        """Load products and reviews data."""
        # Load comprehensive Sephora products
        products_path = "data/silver/sephora_products.parquet"
        if Path(products_path).exists():
            self.products_df = pd.read_parquet(products_path)
            logger.info(f"Loaded {len(self.products_df)} products")
        else:
            logger.warning(f"Products file not found: {products_path}")
            self.products_df = pd.DataFrame()
        
        # Load comprehensive Sephora reviews
        reviews_path = "data/silver/sephora_reviews.parquet"
        if Path(reviews_path).exists():
            self.reviews_df = pd.read_parquet(reviews_path)
            logger.info(f"Loaded {len(self.reviews_df)} reviews")
        else:
            logger.warning(f"Reviews file not found: {reviews_path}")
            self.reviews_df = pd.DataFrame()
    
    def create_behavioral_features(self) -> pd.DataFrame:
        """Create behavioral features (RFM-like on reviews)."""
        if self.reviews_df.empty or self.products_df.empty:
            return pd.DataFrame()
        
        # Aggregate review metrics by product
        review_features = self.reviews_df.groupby('product_id').agg({
            'rating': ['count', 'mean', 'std'],
            'helpful_count': 'mean',
            'verified_purchase': 'mean',
            'language': lambda x: (x == 'fr').mean()
        }).reset_index()
        
        # Flatten column names
        review_features.columns = [
            'product_id', 'review_count', 'avg_rating', 'rating_std',
            'helpful_avg', 'verified_share', 'french_ratio'
        ]
        
        # Fill NaN values
        review_features = review_features.fillna({
            'rating_std': 0,
            'helpful_avg': 0,
            'verified_share': 0,
            'french_ratio': 0
        })
        
        logger.info(f"Created behavioral features for {len(review_features)} products")
        return review_features
    
    def create_price_assortment_features(self) -> pd.DataFrame:
        """Create price and assortment context features."""
        if self.products_df.empty:
            return pd.DataFrame()
        
        # Price features
        price_features = self.products_df[['product_id', 'price_value', 'size_ml_or_g']].copy()
        
        # Z-score within category
        price_features['z_price_in_category'] = price_features.groupby(
            self.products_df['category_path'].str[0]
        )['price_value'].transform(lambda x: (x - x.mean()) / x.std())
        
        # Price per ml/g
        price_features['price_per_unit'] = price_features['price_value'] / price_features['size_ml_or_g']
        
        # Availability (simplified - assume all available for sample data)
        price_features['availability_rate'] = 1.0
        
        # Newness proxy (days since first seen)
        if 'first_seen_ts' in self.products_df.columns:
            price_features['newness_proxy'] = (
                datetime.now() - pd.to_datetime(self.products_df['first_seen_ts'])
            ).dt.days
        else:
            price_features['newness_proxy'] = 0
        
        logger.info(f"Created price/assortment features for {len(price_features)} products")
        return price_features
    
    def create_sustainability_features(self) -> pd.DataFrame:
        """Create sustainability block features."""
        if self.products_df.empty:
            return pd.DataFrame()
        
        sustainability_features = self.products_df[['product_id', 'brand', 'refillable_flag']].copy()
        
        # Refillable flag
        sustainability_features['refillable_flag'] = sustainability_features['refillable_flag'].astype(int)
        
        # Brand-level refill share
        brand_refill_share = self.products_df.groupby('brand')['refillable_flag'].mean().reset_index()
        brand_refill_share.columns = ['brand', 'refill_shelf_share']
        
        sustainability_features = sustainability_features.merge(
            brand_refill_share, on='brand', how='left'
        )
        
        # Eco badge count (simplified - count refill evidence types)
        def count_eco_evidence(evidence_list):
            if evidence_list is None or len(evidence_list) == 0:
                return 0
            return len(evidence_list)
        
        sustainability_features['eco_badge_count'] = self.products_df['refill_evidence'].apply(count_eco_evidence)
        
        logger.info(f"Created sustainability features for {len(sustainability_features)} products")
        return sustainability_features
    
    def create_text_features(self) -> pd.DataFrame:
        """Create text embedding features from French reviews."""
        if self.reviews_df.empty:
            return pd.DataFrame()
        
        # Filter French reviews
        french_reviews = self.reviews_df[self.reviews_df['language'] == 'fr'].copy()
        
        if len(french_reviews) == 0:
            logger.warning("No French reviews found for text features")
            return pd.DataFrame()
        
        # Combine review text by product
        product_texts = french_reviews.groupby('product_id')['body'].apply(
            lambda x: ' '.join(x.astype(str))
        ).reset_index()
        
        # Clean text
        def clean_text(text):
            if pd.isna(text):
                return ""
            # Remove special characters, keep French accents
            text = re.sub(r'[^\w\sàâäéèêëïîôöùûüÿç]', ' ', str(text))
            return text.lower().strip()
        
        product_texts['clean_text'] = product_texts['body'].apply(clean_text)
        
        # Create TF-IDF features
        try:
            tfidf_matrix = self.text_vectorizer.fit_transform(product_texts['clean_text'])
            tfidf_df = pd.DataFrame(
                tfidf_matrix.toarray(),
                columns=[f'text_feature_{i}' for i in range(tfidf_matrix.shape[1])]
            )
            tfidf_df['product_id'] = product_texts['product_id'].values
            
            # Reduce dimensionality with PCA
            if tfidf_df.shape[1] > 20:
                pca = PCA(n_components=20, random_state=42)
                text_features = pca.fit_transform(tfidf_df.drop('product_id', axis=1))
                text_features_df = pd.DataFrame(
                    text_features,
                    columns=[f'txt_dim_{i:02d}' for i in range(20)]
                )
                text_features_df['product_id'] = tfidf_df['product_id'].values
            else:
                text_features_df = tfidf_df
            
            logger.info(f"Created text features for {len(text_features_df)} products")
            return text_features_df
            
        except Exception as e:
            logger.warning(f"Text feature creation failed: {e}")
            return pd.DataFrame()
    
    def create_luxury_classification_features(self) -> pd.DataFrame:
        """Create luxury classification features."""
        if self.products_df.empty:
            return pd.DataFrame()
        
        luxury_features = self.products_df[['product_id', 'is_luxury', 'brand_tier']].copy()
        
        # Convert brand tier to numeric
        luxury_features['brand_tier_numeric'] = luxury_features['brand_tier'].map({
            '1': 1,
            '1.5': 1.5
        }).fillna(0)
        
        # Luxury flag
        luxury_features['is_luxury'] = luxury_features['is_luxury'].astype(int)
        
        logger.info(f"Created luxury features for {len(luxury_features)} products")
        return luxury_features
    
    def build_feature_matrix(self) -> pd.DataFrame:
        """Build complete feature matrix for clustering."""
        logger.info("Building feature matrix for clustering...")
        
        if self.products_df.empty:
            logger.warning("No products data available")
            return pd.DataFrame()
        
        # Start with product IDs
        feature_matrix = self.products_df[['product_id']].copy()
        
        # Add each feature block
        feature_blocks = [
            ('behavioral', self.create_behavioral_features()),
            ('price_assortment', self.create_price_assortment_features()),
            ('sustainability', self.create_sustainability_features()),
            ('luxury', self.create_luxury_classification_features())
        ]
        
        for block_name, features_df in feature_blocks:
            if not features_df.empty:
                feature_matrix = feature_matrix.merge(
                    features_df, on='product_id', how='left'
                )
                logger.info(f"Added {block_name} features")
        
        # Add text features if available
        text_features = self.create_text_features()
        if not text_features.empty:
            feature_matrix = feature_matrix.merge(
                text_features, on='product_id', how='left'
            )
            logger.info("Added text features")
        
        # Fill NaN values
        numeric_columns = feature_matrix.select_dtypes(include=[np.number]).columns
        feature_matrix[numeric_columns] = feature_matrix[numeric_columns].fillna(0)
        
        # Remove product_id and non-numeric columns for clustering
        clustering_features = feature_matrix.drop(['product_id', 'brand'], axis=1, errors='ignore')
        
        # Keep only numeric columns
        clustering_features = clustering_features.select_dtypes(include=[np.number])
        
        # Standardize features
        clustering_features_scaled = pd.DataFrame(
            self.scaler.fit_transform(clustering_features),
            columns=clustering_features.columns,
            index=clustering_features.index
        )
        
        # Add back product_id
        clustering_features_scaled['product_id'] = feature_matrix['product_id']
        
        logger.info(f"Built feature matrix with {len(clustering_features_scaled)} products and {len(clustering_features_scaled.columns)-1} features")
        
        return clustering_features_scaled
    
    def save_feature_matrix(self, feature_matrix: pd.DataFrame, output_path: str = "data/silver/feature_matrix.parquet"):
        """Save feature matrix to file."""
        feature_matrix.to_parquet(output_path, index=False)
        logger.info(f"Feature matrix saved to {output_path}")
        
        # Also save feature descriptions
        feature_descriptions = {
            'feature_count': len(feature_matrix.columns) - 1,  # Exclude product_id
            'product_count': len(feature_matrix),
            'features': list(feature_matrix.columns),
            'created_at': datetime.now().isoformat()
        }
        
        with open("data/silver/feature_descriptions.json", "w") as f:
            json.dump(feature_descriptions, f, indent=2)
        
        logger.info("Feature descriptions saved")

def main():
    """Main execution function."""
    engineer = FeatureEngineer()
    engineer.load_data()
    
    if engineer.products_df.empty:
        logger.warning("No data available for feature engineering")
        return
    
    # Build feature matrix
    feature_matrix = engineer.build_feature_matrix()
    
    if not feature_matrix.empty:
        # Save feature matrix
        engineer.save_feature_matrix(feature_matrix)
        
        # Print summary
        print("\n" + "="*60)
        print("FEATURE ENGINEERING COMPLETE")
        print("="*60)
        print(f"📊 Products: {len(feature_matrix)}")
        print(f"🔧 Features: {len(feature_matrix.columns) - 1}")  # Exclude product_id
        print(f"📁 Saved: data/silver/feature_matrix.parquet")
        print("="*60)
        
        # Show feature list
        features = [col for col in feature_matrix.columns if col != 'product_id']
        print("\nFeatures created:")
        for i, feature in enumerate(features, 1):
            print(f"  {i:2d}. {feature}")
    else:
        logger.warning("Feature matrix creation failed")

if __name__ == "__main__":
    main()
