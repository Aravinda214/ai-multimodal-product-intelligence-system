from pathlib import Path
import pandas as pd

BASE_DIR = Path(".")
OUT_DIR = BASE_DIR / "out_native"

# If you want to OVERWRITE the existing files, set this to False.
# If True, it will create new files with "_single" suffix.
CREATE_NEW_FILES = True

CATEGORIES = [
    "Electronics",
    "Health_and_Household",
    "Clothing_Shoes_and_Jewelry",
]


def pick_single_product(top_path: Path) -> str:
    """
    Read top_products_<cat>.csv, drop NaN/empty descriptions,
    and return the product_id of the product with the highest review_count.
    Also return the filtered single-row DataFrame.
    """
    df = pd.read_csv(top_path)

    # Ensure columns exist
    if "description_text" not in df.columns:
        raise ValueError(f"{top_path} does not have 'description_text' column")

    # Drop NaN or empty descriptions
    valid = df[df["description_text"].notna()].copy()
    valid = valid[valid["description_text"].str.strip() != ""]

    if valid.empty:
        raise ValueError(f"No products with non-empty description_text in {top_path}")

    # Pick product with max review_count
    valid = valid.sort_values("review_count", ascending=False)
    best_row = valid.iloc[0]
    product_id = best_row["product_id"]

    # Single-row dataframe
    single_df = valid[valid["product_id"] == product_id].copy()
    return product_id, single_df


def process_category(cat: str):
    print(f"\n=== Category: {cat} ===")
    cat_safe = cat  # already safe because of naming

    top_path = OUT_DIR / f"top_products_{cat_safe}.csv"
    reviews_path = OUT_DIR / f"cleaned_reviews_{cat_safe}.parquet"

    if not top_path.exists():
        print(f"  ! Missing CSV: {top_path}")
        return
    if not reviews_path.exists():
        print(f"  ! Missing parquet: {reviews_path}")
        return

    # 1) Choose the single product
    product_id, single_top_df = pick_single_product(top_path)
    print(f"  -> Chosen product_id: {product_id}")

    # 2) Filter reviews parquet to this product only
    reviews_df = pd.read_parquet(reviews_path)
    single_reviews_df = reviews_df[reviews_df["product_id"] == product_id].copy()
    print(f"  -> Reviews kept for this product: {len(single_reviews_df)}")

    # 3) Save results
    if CREATE_NEW_FILES:
        top_out = OUT_DIR / f"top_product_{cat_safe}_single.csv"
        reviews_out = OUT_DIR / f"cleaned_reviews_{cat_safe}_single.parquet"
    else:
        # overwrite original files
        top_out = top_path
        reviews_out = reviews_path

    single_top_df.to_csv(top_out, index=False)
    single_reviews_df.to_parquet(reviews_out, index=False)

    print(f"  -> Saved single-product CSV to: {top_out}")
    print(f"  -> Saved single-product reviews parquet to: {reviews_out}")


if __name__ == "__main__":
    for cat in CATEGORIES:
        process_category(cat)

    print("\nDONE ✅ Reduced to 1 product per category.")
