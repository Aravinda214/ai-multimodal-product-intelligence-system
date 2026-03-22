import pandas as pd

# Configure pandas to show full text (remove truncation)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', 10)

# Load Electronics reviews
reviews_elec = pd.read_parquet("out_native/cleaned_reviews_Electronics.parquet")

print("=== ELECTRONICS REVIEWS OVERVIEW ===")
print(f"Total reviews: {len(reviews_elec):,}")
print(f"Columns: {list(reviews_elec.columns)}")

print("\n=== TOP 10 PRODUCTS BY REVIEW COUNT ===")
product_counts = reviews_elec['product_id'].value_counts().head(10)
print(product_counts)

print("\n=== REVIEWS FOR TOP PRODUCT (FULL TEXT) ===")
top_product_id = product_counts.index[0]
print(f"Product ID: {top_product_id}")
top_product_reviews = reviews_elec[reviews_elec['product_id'] == top_product_id]
print(f"Number of reviews: {len(top_product_reviews)}")

print("\nSample reviews for this product (showing full text):")
for i, (idx, row) in enumerate(top_product_reviews[['rating', 'review_title', 'text']].head(3).iterrows()):
    print(f"\n--- Review {i+1} ---")
    print(f"Rating: {row['rating']}")
    print(f"Title: {row['review_title']}")
    print(f"Text: {row['text']}")
    print("-" * 80)

print("\n=== AVAILABLE FILES ===")
import os
out_files = [f for f in os.listdir("out_native") if f.endswith(('.csv', '.parquet'))]
print("Files in out_native directory:")
for f in out_files:
    print(f"  - {f}")
