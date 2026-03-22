# """
# select_top_products.py

# Your job: run this locally to:
# - Load Amazon Reviews 2023 JSONL files for:
#     Electronics
#     Health_and_Household
#     Clothing_Shoes_and_Jewelry
# - Clean and aggregate reviews
# - Join with metadata
# - Select top products per category
# - Save:
#     out/top_products_<category>.csv
#     out/cleaned_reviews_<category>.parquet
# """

# import json
# from pathlib import Path
# from typing import List, Dict, Optional

# import pandas as pd


# # ================= CONFIG =================

# # Folder where your JSONL files live
# DATA_DIR = Path(".")  # change if needed, e.g. Path("/path/to/amazon_data")
# OUTPUT_DIR = Path("./out")
# OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# # Make sure these filenames match what you actually downloaded
# CATEGORIES = {
#     "Electronics": {
#         "reviews_file": "Electronics.jsonl/Electronics.jsonl",
#         "meta_file": "meta_Electronics.jsonl/meta_Electronics.jsonl",
#     },
#     "Health_and_Household": {
#         "reviews_file": "Health_and_Household.jsonl/Health_and_Household.jsonl",
#         "meta_file": "meta_Health_and_Household.jsonl/meta_Health_and_Household.jsonl",
#     },
#     "Clothing_Shoes_and_Jewelry": {
#         "reviews_file": "Clothing_Shoes_and_Jewelry.jsonl/Clothing_Shoes_and_Jewelry.jsonl",
#         "meta_file": "meta_Clothing_Shoes_and_Jewelry.jsonl/meta_Clothing_Shoes_and_Jewelry.jsonl",
#     },
# }

# # How many products to keep *per category* in the shortlist
# TOP_N_PRODUCTS = 10        # gives you 10 candidates; you can manually pick 2–3 later

# # Minimum number of reviews to consider a product
# MIN_REVIEWS_PER_PRODUCT = 30


# # ================= HELPERS =================

# def load_jsonl_to_df(path: Path, columns_subset: Optional[List[str]] = None, chunksize: int = 10000) -> pd.DataFrame:
#     """
#     Load a JSONL file into a pandas DataFrame in chunks to handle large files.
#     Reads line-by-line to avoid memory issues.
#     """
#     print(f"\nLoading {path} line-by-line...")
#     if not path.exists():
#         raise FileNotFoundError(f"File not found: {path}")

#     chunks = []
#     chunk_data = []
#     total_rows = 0
    
#     with open(path, 'r', encoding='utf-8') as f:
#         for i, line in enumerate(f, 1):
#             try:
#                 record = json.loads(line.strip())
                
#                 # Filter columns if needed
#                 if columns_subset is not None:
#                     record = {k: v for k, v in record.items() if k in columns_subset}
                
#                 chunk_data.append(record)
                
#                 # Process in smaller chunks
#                 if len(chunk_data) >= chunksize:
#                     df_chunk = pd.DataFrame(chunk_data)
#                     chunks.append(df_chunk)
#                     total_rows += len(chunk_data)
#                     print(f"  Loaded {total_rows:,} rows so far...")
#                     chunk_data = []
                    
#                     # Periodically combine chunks to free memory
#                     if len(chunks) >= 50:
#                         print(f"  Consolidating chunks...")
#                         combined = pd.concat(chunks, ignore_index=True)
#                         chunks = [combined]
                    
#             except json.JSONDecodeError:
#                 print(f"  Warning: Skipping invalid JSON at line {i}")
#                 continue
    
#     # Add remaining data
#     if chunk_data:
#         chunks.append(pd.DataFrame(chunk_data))
#         total_rows += len(chunk_data)
    
#     df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
#     print(f"Loaded {len(df):,} rows from {path.name}")
#     return df


# def clean_reviews_df(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Basic cleaning:
#     - Ensure key columns exist
#     - Drop empty texts
#     - Create product_id (parent_asin or asin)
#     - Drop tiny reviews (very short)
#     - Drop duplicates
#     """
#     for col in ["text", "asin", "parent_asin"]:
#         if col not in df.columns:
#             df[col] = None

#     # Text cleaning
#     df["text"] = df["text"].astype(str).str.strip()
#     df = df[df["text"] != ""].copy()

#     # Unified product_id
#     df["product_id"] = df["parent_asin"].fillna(df["asin"])
#     df = df[df["product_id"].notna()].copy()

#     # Filter out super short reviews (e.g., < 3 words)
#     df = df[df["text"].str.split().str.len() >= 3].copy()

#     # Drop duplicates
#     dup_cols = [c for c in ["user_id", "product_id", "text"] if c in df.columns]
#     if dup_cols:
#         before = len(df)
#         df = df.drop_duplicates(subset=dup_cols)
#         after = len(df)
#         print(f"Removed {before - after:,} duplicate reviews")

#     print(f"After cleaning: {len(df):,} reviews remain")
#     return df


# def aggregate_reviews(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Aggregate review stats at product level.
#     """
#     for col in ["rating", "helpful_vote"]:
#         if col not in df.columns:
#             df[col] = None

#     agg = (
#         df.groupby("product_id")
#         .agg(
#             review_count=("rating", "count"),
#             avg_rating=("rating", "mean"),
#             avg_helpful_votes=("helpful_vote", "mean"),
#         )
#         .reset_index()
#     )

#     print(f"Aggregated stats for {len(agg):,} products")
#     return agg


# def clean_meta_df(meta_df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Clean metadata and prepare for merge.
#     """
#     if "parent_asin" not in meta_df.columns:
#         meta_df["parent_asin"] = None

#     meta_df["product_id"] = meta_df["parent_asin"]

#     # has_image flag
#     def has_image(images):
#         return bool(isinstance(images, list) and len(images) > 0)

#     if "images" not in meta_df.columns:
#         meta_df["images"] = None

#     meta_df["has_image"] = meta_df["images"].apply(has_image)

#     # Flatten description -> description_text
#     if "description" in meta_df.columns:
#         def desc_to_str(d):
#             if isinstance(d, list):
#                 return " ".join(str(x) for x in d if x)
#             return str(d) if d is not None else ""
#         meta_df["description_text"] = meta_df["description"].apply(desc_to_str)
#     else:
#         meta_df["description_text"] = ""

#     keep_cols = [
#         "product_id",
#         "main_category",
#         "title",
#         "average_rating",
#         "rating_number",
#         "price",
#         "has_image",
#         "description_text",
#         "images",
#     ]
#     existing = [c for c in keep_cols if c in meta_df.columns]
#     meta_df = meta_df[existing].copy()

#     return meta_df


# def select_top_products(
#     reviews_df: pd.DataFrame,
#     meta_df: pd.DataFrame,
#     top_n: int,
#     min_reviews: int,
# ) -> tuple[pd.DataFrame, pd.DataFrame]:
#     """
#     - Aggregate reviews
#     - Merge with metadata
#     - Filter by min_reviews and presence of image/description
#     - Sort and select top_n
#     - Return (top_products_df, reviews_for_top_df)
#     """
#     agg_df = aggregate_reviews(reviews_df)
#     meta_clean = clean_meta_df(meta_df)

#     merged = agg_df.merge(meta_clean, on="product_id", how="left")

#     # Filter for interesting products
#     merged = merged[merged["review_count"] >= min_reviews].copy()
#     merged = merged[
#         (merged["has_image"] == True)
#         | (merged["description_text"].str.strip() != "")
#     ].copy()

#     # Sort by review_count desc, then avg_rating desc
#     merged = merged.sort_values(
#         by=["review_count", "avg_rating"],
#         ascending=[False, False]
#     )

#     top_products_df = merged.head(top_n).reset_index(drop=True)

#     print("\nTop products candidate list:")
#     print(top_products_df[["product_id", "title", "review_count", "avg_rating"]].head(top_n))

#     selected_ids = set(top_products_df["product_id"].tolist())
#     reviews_for_top = reviews_df[reviews_df["product_id"].isin(selected_ids)].copy()

#     return top_products_df, reviews_for_top


# # ================= MAIN PIPELINE =================

# def process_category(category_name: str, config: Dict[str, str]) -> None:
#     """
#     Run full pipeline for one category.
#     """
#     print("\n" + "=" * 80)
#     print(f"Processing category: {category_name}")
#     print("=" * 80)

#     reviews_path = DATA_DIR / config["reviews_file"]
#     meta_path = DATA_DIR / config["meta_file"]

#     # Load reviews
#     reviews_cols = [
#         "rating",
#         "title",
#         "text",
#         "images",
#         "asin",
#         "parent_asin",
#         "user_id",
#         "timestamp",
#         "helpful_vote",
#         "verified_purchase",
#     ]
#     reviews_df = load_jsonl_to_df(reviews_path, columns_subset=reviews_cols)

#     # Load metadata
#     meta_df = load_jsonl_to_df(meta_path)

#     # Clean reviews
#     reviews_clean = clean_reviews_df(reviews_df)

#     # Select top products & corresponding reviews
#     top_products_df, reviews_for_top = select_top_products(
#         reviews_clean,
#         meta_df,
#         top_n=TOP_N_PRODUCTS,
#         min_reviews=MIN_REVIEWS_PER_PRODUCT,
#     )

#     # Save outputs
#     cat_safe = category_name.replace(" ", "_")
#     top_products_file = OUTPUT_DIR / f"top_products_{cat_safe}.csv"
#     reviews_file = OUTPUT_DIR / f"cleaned_reviews_{cat_safe}.parquet"

#     top_products_df.to_csv(top_products_file, index=False)
#     reviews_for_top.to_parquet(reviews_file, index=False)

#     print(f"\nSaved top products to: {top_products_file}")
#     print(f"Saved cleaned reviews for top products to: {reviews_file}")


# if __name__ == "__main__":
#     for cat_name, cfg in CATEGORIES.items():
#         process_category(cat_name, cfg)

#     print("\nDONE ✅ All categories processed.")

"""
select_top_products_native.py

Streams local JSONL files WITHOUT loading them fully into memory.
For each category:
  1) Stream reviews, count review_count per product_id.
  2) Select top N products with at least MIN_REVIEWS_PER_PRODUCT.
  3) Stream metadata, keep only those top products.
  4) Stream reviews again, keep only top products.
  5) Save:
       out_native/top_products_<category>.csv
       out_native/cleaned_reviews_<category>.parquet
"""

import json
from collections import Counter
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


# ================= CONFIG =================

DATA_DIR = Path(".")  # folder where your *.jsonl live
OUTPUT_DIR = Path("out_native")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CATEGORIES = {
    "Electronics": {
        "reviews_file": "Electronics.jsonl/Electronics.jsonl",
        "meta_file": "meta_Electronics.jsonl/meta_Electronics.jsonl",
    },
    "Health_and_Household": {
        "reviews_file": "Health_and_Household.jsonl/Health_and_Household.jsonl",
        "meta_file": "meta_Health_and_Household.jsonl/meta_Health_and_Household.jsonl",
    },
    "Clothing_Shoes_and_Jewelry": {
        "reviews_file": "Clothing_Shoes_and_Jewelry.jsonl/Clothing_Shoes_and_Jewelry.jsonl",
        "meta_file": "meta_Clothing_Shoes_and_Jewelry.jsonl/meta_Clothing_Shoes_and_Jewelry.jsonl",
    },
}

TOP_N_PRODUCTS = 10          # shortlist size; you can later pick 2–3
MIN_REVIEWS_PER_PRODUCT = 30 # ignore products with fewer reviews

# For debugging / faster iteration you can limit how many lines you scan:
MAX_REVIEWS_TO_SCAN = None   # e.g., 5_000_000 or None for full file


# ================= HELPERS =================

def iter_jsonl(path: Path):
    """Yield parsed JSON objects from a JSONL file, line by line."""
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def extract_product_id(ex: Dict) -> str:
    """Unified product ID: prefer parent_asin, fall back to asin."""
    return ex.get("parent_asin") or ex.get("asin")


def is_valid_review(ex: Dict) -> bool:
    """Filter out empty or ultra-short reviews."""
    text = (ex.get("text") or "").strip()
    if not text:
        return False
    if len(text.split()) < 3:
        return False
    return True


def first_pass_count_reviews(reviews_path: Path) -> Counter:
    """
    Stream through reviews JSONL and count valid reviews per product_id.
    """
    print(f"  -> First pass: counting reviews in {reviews_path.name}")
    counts = Counter()
    for i, ex in enumerate(iter_jsonl(reviews_path)):
        if MAX_REVIEWS_TO_SCAN is not None and i >= MAX_REVIEWS_TO_SCAN:
            break

        if not is_valid_review(ex):
            continue

        pid = extract_product_id(ex)
        if not pid:
            continue

        counts[pid] += 1

        if (i + 1) % 1_000_000 == 0:
            print(f"     Processed {i+1:,} review lines...")

    print(f"  -> Finished counting: {len(counts):,} products with at least 1 valid review")
    return counts


def select_top_product_ids(counts: Counter) -> List[Tuple[str, int]]:
    """
    Filter by MIN_REVIEWS_PER_PRODUCT and return top N (product_id, review_count).
    """
    filtered = [(pid, c) for pid, c in counts.items() if c >= MIN_REVIEWS_PER_PRODUCT]
    filtered.sort(key=lambda x: x[1], reverse=True)
    top = filtered[:TOP_N_PRODUCTS]

    print(f"  -> Products with >= {MIN_REVIEWS_PER_PRODUCT} reviews: {len(filtered):,}")
    print(f"  -> Top {len(top)} products:")
    for pid, c in top:
        print(f"       {pid}: {c} reviews")

    return top


def collect_metadata_for_top(meta_path: Path, top_product_ids: List[str]) -> pd.DataFrame:
    """
    Stream metadata JSONL and keep entries whose parent_asin in top_product_ids.
    """
    print(f"  -> Streaming metadata from {meta_path.name}")
    top_set = set(top_product_ids)
    rows = []

    for ex in iter_jsonl(meta_path):
        pid = ex.get("parent_asin")
        if pid not in top_set:
            continue

        desc = ex.get("description")
        if isinstance(desc, list):
            desc_text = " ".join(str(x) for x in desc if x)
        else:
            desc_text = str(desc) if desc is not None else ""

        images = ex.get("images")
        has_image = bool(isinstance(images, list) and len(images) > 0)

        rows.append(
            {
                "product_id": pid,
                "title": ex.get("title"),
                "main_category": ex.get("main_category"),
                "average_rating_meta": ex.get("average_rating"),
                "rating_number_meta": ex.get("rating_number"),
                "price": ex.get("price"),
                "description_text": desc_text,
                "has_image": has_image,
                "raw_images": images,
            }
        )

    meta_df = pd.DataFrame(rows)
    print(f"  -> Found metadata for {len(meta_df):,} of {len(top_product_ids)} top products")
    return meta_df


def second_pass_collect_reviews_for_top(
    reviews_path: Path,
    top_product_ids: List[str],
) -> pd.DataFrame:
    """
    Stream reviews again and keep only reviews for products in top_product_ids.
    """
    print(f"  -> Second pass: collecting reviews for top products from {reviews_path.name}")
    top_set = set(top_product_ids)
    rows = []

    for i, ex in enumerate(iter_jsonl(reviews_path)):
        if not is_valid_review(ex):
            continue

        pid = extract_product_id(ex)
        if pid not in top_set:
            continue

        rows.append(
            {
                "product_id": pid,
                "rating": ex.get("rating"),
                "review_title": ex.get("title"),
                "text": (ex.get("text") or "").strip(),
                "helpful_vote": ex.get("helpful_vote"),
                "user_id": ex.get("user_id"),
                "timestamp": ex.get("timestamp"),
                "verified_purchase": ex.get("verified_purchase"),
            }
        )

    df = pd.DataFrame(rows)
    print(f"  -> Collected {len(df):,} reviews for top products")
    return df


# ================= MAIN PIPELINE =================

def process_category(cat_name: str, cfg: Dict[str, str]) -> None:
    print("\n" + "=" * 80)
    print(f"Processing category: {cat_name}")
    print("=" * 80)

    reviews_path = DATA_DIR / cfg["reviews_file"]
    meta_path = DATA_DIR / cfg["meta_file"]

    if not reviews_path.exists():
        print(f"  !! Reviews file not found: {reviews_path}")
        return
    if not meta_path.exists():
        print(f"  !! Meta file not found: {meta_path}")
        return

    # 1) First pass - count reviews per product
    counts = first_pass_count_reviews(reviews_path)

    # 2) Select top products
    top_list = select_top_product_ids(counts)
    if not top_list:
        print("  -> No products passed the MIN_REVIEWS_PER_PRODUCT threshold. Skipping.")
        return

    top_product_ids = [pid for pid, _ in top_list]

    # 3) Collect metadata for those top products
    meta_df = collect_metadata_for_top(meta_path, top_product_ids)

    # 4) Second pass - collect reviews for top products
    reviews_df = second_pass_collect_reviews_for_top(reviews_path, top_product_ids)

    # 5) Merge counts + metadata
    counts_df = pd.DataFrame(top_list, columns=["product_id", "review_count"])
    top_products_df = counts_df.merge(meta_df, on="product_id", how="left")

    # 6) Save outputs
    cat_safe = cat_name.replace(" ", "_")
    top_products_out = OUTPUT_DIR / f"top_products_{cat_safe}.csv"
    reviews_out = OUTPUT_DIR / f"cleaned_reviews_{cat_safe}.parquet"

    top_products_df.to_csv(top_products_out, index=False)
    reviews_df.to_parquet(reviews_out, index=False)

    print(f"  -> Saved top products to: {top_products_out}")
    print(f"  -> Saved cleaned reviews for top products to: {reviews_out}")


if __name__ == "__main__":
    for cat, cfg in CATEGORIES.items():
        process_category(cat, cfg)

    print("\nDONE ✅ All categories processed.")
