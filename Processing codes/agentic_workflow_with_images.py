"""
Minimal Agentic Workflow for 94-844 Final Project (Bonus Q4)

This script shows an AI "agentic" pipeline that connects:
- Q1: Product description → structured attributes
- Q2: Reviews → topics & sentiments
- Q3: Prompts for image generation
- Q4: Image generation using DALL-E and Gemini

Agents:
- ProductSelectionAgent
- DescriptionAgent
- ReviewAgent
- PromptBuilderAgent
- ImageGenerationAgent
- OrchestratorAgent (Coordinator)
"""

import json
import os
import random
import base64
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# ----------------------------------------------------------------------
# API client setup - OpenAI + Google AI (Gemini)
# Set your API keys as environment variables or in .env file:
# - OPENAI_API_KEY for DALL-E
# - GOOGLE_API_KEY for Gemini Flash 2.5
# ----------------------------------------------------------------------
try:
    from openai import OpenAI
    import os
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # OpenAI setup
    openai_api_key = os.getenv('OPENAI_API_KEY')
    if not openai_api_key:
        print("WARNING: OPENAI_API_KEY not found. Set it as environment variable or in .env file.")
        OPENAI_CLIENT = None
    else:
        OPENAI_CLIENT: Optional[OpenAI] = OpenAI(api_key=openai_api_key)
        print("✓ OpenAI client initialized successfully")
    
    # Google AI setup for Gemini
    google_api_key = os.getenv('GOOGLE_API_KEY')
    GOOGLE_CLIENT = None
    try:
        if google_api_key:
            import google.generativeai as genai
            genai.configure(api_key=google_api_key)
            GOOGLE_CLIENT = genai
            print("✓ Google AI client initialized successfully")
            print("  Available: Gemini 2.5 Flash Image → Gemini 3 Pro Image → Imagen fallbacks")
        else:
            print("WARNING: GOOGLE_API_KEY not found. Gemini image generation will be disabled.")
    except ImportError:
        print("WARNING: google-generativeai not installed. Install with: pip install google-generativeai")
        GOOGLE_CLIENT = None
    
    DEFAULT_MODEL = "gpt-4o"  # Use GPT-4o (latest available model)
except ImportError:
    print("ERROR: Missing required packages. Install with: pip install openai python-dotenv google-generativeai requests")
    OPENAI_CLIENT = None
    GOOGLE_CLIENT = None
    DEFAULT_MODEL = None
except Exception as e:
    print(f"ERROR setting up API clients: {e}")
    OPENAI_CLIENT = None
    GOOGLE_CLIENT = None
    DEFAULT_MODEL = None


def call_llm(system_prompt: str, user_prompt: str, model: Optional[str] = None) -> str:
    """Helper to call LLM with fallback to dummy response."""
    if OPENAI_CLIENT is None:
        return "[LLM disabled in this environment – dummy response used for testing.]"

    model = model or DEFAULT_MODEL
    try:
        resp = OPENAI_CLIENT.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
        )
        return resp.choices[0].message.content
    except Exception as e:
        print(f"[LLM ERROR] {e}")
        return "[LLM error – placeholder response.]"


# ------------------------------------------------------------
# Image generation helpers
# ------------------------------------------------------------
IMAGES_DIR = Path("outputs") / "images"
IMAGES_DIR.mkdir(parents=True, exist_ok=True)


def _safe_filename(text: str, max_len: int = 40) -> str:
    """Turn a product title into a filesystem-safe prefix."""
    cleaned = "".join(
        c.lower() if c.isalnum() else "_" for c in text.strip()
    )
    cleaned = "_".join([p for p in cleaned.split("_") if p])
    return cleaned[:max_len] if cleaned else "product"


def _truncate_prompt(prompt: str, max_length: int = 3800) -> str:
    """Truncate prompt to fit DALL-E's 4000 character limit with safety margin."""
    if len(prompt) <= max_length:
        return prompt
    
    # Keep the beginning and add truncation notice
    truncated = prompt[:max_length - 100]  # Leave room for truncation notice
    truncated += "\n\n[Note: Prompt truncated to fit length requirements. Focus on key visual elements described above.]"
    
    print(f"  [ImageAgent] Prompt truncated from {len(prompt)} to {len(truncated)} characters")
    return truncated


def _generate_single_dalle_image(client, prompt: str, out_path: Path, retries: int = 3):
    """Call OpenAI images API with GPT-4o model, with simple retry, and save PNG to out_path."""
    # Truncate prompt if too long
    safe_prompt = _truncate_prompt(prompt)
    
    for attempt in range(1, retries + 1):
        try:
            resp = client.images.generate(
                model="dall-e-3",  # Note: DALL-E 3 is the image model, GPT-4o is for text
                prompt=safe_prompt,
                size="1024x1024",
                quality="hd",  # Use high quality for better results
                n=1,
            )
            # Download and save the image
            import requests
            img_url = resp.data[0].url
            img_response = requests.get(img_url)
            with open(out_path, "wb") as f:
                f.write(img_response.content)
            print(f"  ✓ DALL-E 3 (GPT-4o backend) image saved: {out_path.name}")
            return True
        except Exception as e:
            print(f"  [ImageAgent] DALL·E attempt {attempt} failed: {e}")
            if attempt == retries:
                return False
            time.sleep(2 * attempt)


def _generate_single_gemini_image(prompt: str, out_path: Path, retries: int = 2):
    """Generate image using Gemini image generation models following official documentation."""
    if GOOGLE_CLIENT is None:
        print(f"  [ImageAgent] Skipping Gemini - client not available")
        return False
        
    # Only the two official Gemini image models (based on documentation)
    image_models = [
        "gemini-2.5-flash-image",      # Fast and efficient
        "gemini-3-pro-image-preview",  # Advanced model with thinking
    ]
    
    # Try to use the new Google Gen AI SDK first (preferred)
    try:
        from google import genai
        from google.genai import types
        
        # Initialize the new SDK client
        api_key = os.getenv('GOOGLE_API_KEY') or os.getenv('GOOGLE_AI_API_KEY') or os.getenv('GEMINI_API_KEY')
        if not api_key:
            print("  [ImageAgent] No API key found for new SDK")
            raise ImportError("No API key available")
            
        client = genai.Client(api_key=api_key)
        
        for attempt in range(1, retries + 1):
            for model_name in image_models:
                try:
                    image_prompt = _truncate_prompt(prompt, 1500)
                    print(f"  [ImageAgent] Trying {model_name} with new SDK (attempt {attempt})...")
                    
                    # Configure for image generation as per documentation
                    config = types.GenerateContentConfig(
                        response_modalities=['TEXT', 'IMAGE']  # Critical: must specify IMAGE modality
                    )
                    
                    if model_name == "gemini-3-pro-image-preview":
                        # For Gemini 3 Pro, can also specify image config
                        config.image_config = types.ImageConfig(
                            aspect_ratio="1:1",
                            image_size="1K"
                        )
                    
                    response = client.models.generate_content(
                        model=model_name,
                        contents=[image_prompt],
                        config=config
                    )
                    
                    # Process response following official documentation pattern
                    for part in response.parts:
                        if part.text is not None:
                            print(f"  [ImageAgent] {model_name} text response: {part.text[:100]}...")
                            continue
                        elif part.inline_data is not None:
                            # Use the official .as_image() method from documentation
                            image = part.as_image()
                            if image:
                                out_path_png = out_path.with_suffix('.png')
                                image.save(str(out_path_png))
                                print(f"  ✓ {model_name} image generated and saved: {out_path_png.name}")
                                return True
                    
                    print(f"  [ImageAgent] {model_name} did not return image data")
                        
                except Exception as model_err:
                    print(f"  [ImageAgent] {model_name} failed: {str(model_err)[:100]}...")
                    if "quota" in str(model_err).lower() or "limit" in str(model_err).lower():
                        print(f"  [ImageAgent] Rate limit hit, waiting before retry...")
                        time.sleep(5)
                    continue
            
            if attempt < retries:
                print(f"  [ImageAgent] Attempt {attempt} failed, retrying...")
                time.sleep(3)
        
        print(f"  [ImageAgent] New SDK failed after {retries} attempts")
        
    except ImportError as ie:
        print(f"  [ImageAgent] New SDK not available: {ie}")
        print(f"  [ImageAgent] Please install: pip install google-genai")
    except Exception as e:
        print(f"  [ImageAgent] New SDK error: {e}")
    
    # Fallback to legacy SDK if new SDK fails
    print(f"  [ImageAgent] Trying legacy google.generativeai SDK...")
    try:
        for attempt in range(1, retries + 1):
            for model_name in image_models:
                try:
                    image_prompt = _truncate_prompt(prompt, 1500)
                    print(f"  [ImageAgent] Trying {model_name} with legacy SDK (attempt {attempt})...")
                    
                    # Legacy approach (less reliable)
                    model = GOOGLE_CLIENT.GenerativeModel(model_name)
                    response = model.generate_content([image_prompt])
                    
                    # Check for image data in response
                    if hasattr(response, 'parts') and response.parts:
                        for part in response.parts:
                            if hasattr(part, 'inline_data') and part.inline_data and part.inline_data.data:
                                try:
                                    # Try .as_image() method if available
                                    if hasattr(part, 'as_image'):
                                        image = part.as_image()
                                        if image:
                                            out_path_png = out_path.with_suffix('.png')
                                            image.save(str(out_path_png))
                                            print(f"  ✓ {model_name} image saved: {out_path_png.name}")
                                            return True
                                    
                                    # Fallback to manual decoding
                                    image_data = base64.b64decode(part.inline_data.data)
                                    if len(image_data) > 10000:  # Real images should be larger
                                        out_path_png = out_path.with_suffix('.png')
                                        with open(out_path_png, 'wb') as f:
                                            f.write(image_data)
                                        print(f"  ✓ {model_name} image saved: {out_path_png.name}")
                                        return True
                                    else:
                                        print(f"  [ImageAgent] {model_name} returned small data ({len(image_data)} bytes) - likely text")
                                        
                                except Exception as img_err:
                                    print(f"  [ImageAgent] {model_name} image processing error: {img_err}")
                    
                    print(f"  [ImageAgent] {model_name} did not return valid image data")
                        
                except Exception as model_err:
                    print(f"  [ImageAgent] {model_name} legacy failed: {str(model_err)[:100]}...")
                    continue
            
            if attempt < retries:
                print(f"  [ImageAgent] Legacy attempt {attempt} failed, retrying...")
                time.sleep(3)
                
    except Exception as e:
        print(f"  [ImageAgent] Legacy SDK also failed: {e}")
    
    print(f"  [ImageAgent] All Gemini image generation attempts failed")
    return False


# ----------------------------------------------------------------------
# Data structures
# ----------------------------------------------------------------------

@dataclass
class ProductConfig:
    category: str
    meta_path: str
    reviews_path: str


@dataclass
class DescriptionOutput:
    raw_description: str
    llm_response: str


@dataclass
class ReviewOutput:
    n_reviews_sampled: int
    llm_topics_summary: str


@dataclass
class PromptOutput:
    prompt_a: str
    prompt_b: str
    prompt_c: str


@dataclass
class ImageOutput:
    dalle_paths: Dict[str, Optional[str]]
    gemini_paths: Dict[str, Optional[str]]


@dataclass
class ProductRunArtifacts:
    config: ProductConfig
    description: DescriptionOutput
    reviews: ReviewOutput
    prompts: PromptOutput
    images: ImageOutput


# ----------------------------------------------------------------------
# Agents
# ----------------------------------------------------------------------

class ProductSelectionAgent:
    """Reads the single top-product CSV and returns basic metadata."""

    def run(self, cfg: ProductConfig) -> Dict[str, Any]:
        df = pd.read_csv(cfg.meta_path)
        row = df.iloc[0].to_dict()
        return row


class DescriptionAgent:
    """Extracts structured attributes from product description using LLM."""

    SYSTEM_PROMPT = (
        "You are analyzing an Amazon product based on its metadata and description. "
        "Return a concise, machine-readable summary in JSON."
    )

    def run(self, product_meta: Dict[str, Any]) -> DescriptionOutput:
        description = product_meta.get("product_description") or product_meta.get("description") or ""
        title = product_meta.get("product_title") or product_meta.get("title") or ""

        user_prompt = f"""
Here is the product information:

TITLE:
{title}

DESCRIPTION:
{description}

Extract the following in JSON:
- key_features: list of 4–7 short bullet phrases
- strengths: 3–5 things the product claims to do well
- target_user_persona: 2–4 bullet points about who this is for
- value_propositions: 3–5 selling points
- design_attributes: colors, shapes, materials, layout
- performance_dimensions: aspects that customers would care about in reviews
- potential_risks_or_ambiguities: where expectations may not match reality

Return ONLY JSON as text (no backticks).
"""

        llm_response = call_llm(self.SYSTEM_PROMPT, user_prompt)
        return DescriptionOutput(raw_description=description, llm_response=llm_response)


class ReviewAgent:
    """Samples reviews and asks LLM to summarize topics and sentiments."""

    SYSTEM_PROMPT = (
        "You are summarizing customer reviews for an Amazon product. "
        "Focus on the most frequent topics and whether opinions are positive or negative."
    )

    MAX_SAMPLE = 400  # decision rule: cap number of reviews sent to LLM

    def run(self, cfg: ProductConfig) -> ReviewOutput:
        df = pd.read_parquet(cfg.reviews_path)
        
        print(f"Available columns: {list(df.columns)}")
        
        texts = df["text"].dropna().astype(str).tolist()

        if len(texts) == 0:
            return ReviewOutput(
                n_reviews_sampled=0,
                llm_topics_summary="No reviews available.",
            )

        # Simple agentic decision: sample if too many reviews
        if len(texts) > self.MAX_SAMPLE:
            sampled = random.sample(texts, self.MAX_SAMPLE)
        else:
            sampled = texts

        joined = "\\n\\n---\\n\\n".join(sampled)

        user_prompt = f"""
You are given a sample of customer reviews for a single Amazon product.

REVIEWS (sample):
{joined}

Please provide a structured summary:

1. Top 3–5 themes customers talk about (e.g., build quality, sizing, scent, packaging).
2. For each theme, give:
   - short description of what customers say
   - whether sentiment is mostly positive, mostly negative, or mixed
   - any concrete issues or praise patterns (e.g., "print peels after a few washes").
3. One short paragraph: What should a new buyer know before purchasing?

Return the answer as clear bullet points and a short paragraph.
"""

        llm_response = call_llm(self.SYSTEM_PROMPT, user_prompt)
        return ReviewOutput(n_reviews_sampled=len(sampled), llm_topics_summary=llm_response)


class PromptBuilderAgent:
    """Builds three prompt variants (A/B/C) for image generation."""

    def run(
        self,
        cfg: ProductConfig,
        desc: DescriptionOutput,
        reviews: ReviewOutput,
    ) -> PromptOutput:
        product_label = cfg.category.replace("_", " ")

        prompt_a = f"""
You are an image generation model.

Create a clean product image for an Amazon listing in the category "{product_label}".

Base your image ONLY on:
- The product title and description
- The core key_features and design_attributes described here:

{desc.llm_response}

Requirements:
- Neutral studio background.
- Single, centered product.
- No extra props, no people, no busy scenes.
- Do not add any text overlays besides what would appear on the product label itself.
"""

        prompt_b = f"""
You are an image generation model.

Create a more detailed, marketing-style product image for an Amazon listing in the category "{product_label}".

Use:
- The same product attributes as above:
{desc.llm_response}

Emphasize:
- Materials, colors, textures, and realistic lighting.
- A slightly more engaging composition (e.g., light shadows, better angle),
  while keeping the product as the clear focus.

Constraints:
- Keep the physical geometry plausible (correct cables, shapes, placements).
- Avoid adding humans or unrelated decorative items.
"""

        prompt_c = f"""
You are an image generation model.

Create a realistic product image that ALSO reflects common review experiences.

Use:
- Product attributes and design details:
{desc.llm_response}

AND incorporate insights from reviews:
{reviews.llm_topics_summary}

Goal:
- Show the product in a way that still looks like a good listing photo,
  but subtly acknowledges real-world issues reviewers mention
  (for example: slightly worn print, minor packaging scuff, or clear safety label)
  WITHOUT making the product look destroyed or unusable.

Constraints:
- Keep the core product geometry correct.
- Do not exaggerate defects; represent them subtly but clearly.
"""

        return PromptOutput(prompt_a=prompt_a.strip(), prompt_b=prompt_b.strip(), prompt_c=prompt_c.strip())


class ImageGenerationAgent:
    """Generate images for prompts A/B/C using DALL-E and Gemini."""
    
    def run(
        self,
        category: str,
        product_title: str,
        prompts: PromptOutput,
    ) -> Dict[str, Any]:
        """
        Generate images for all three prompts using both models.
        
        Returns dict with structure:
        {
            "dalle": {"A": "path/to/image.png" or None, "B": ..., "C": ...},
            "gemini": {"A": "path/to/image.png" or None, "B": ..., "C": ...}
        }
        """
        print("\\n[ImageAgent] Starting image generation...")
        
        if OPENAI_CLIENT is None:
            print("  [ImageAgent] OpenAI client not available - skipping image generation")
            return {"dalle": {"A": None, "B": None, "C": None}, "gemini": {"A": None, "B": None, "C": None}}
        
        cat_prefix = _safe_filename(category)
        prod_prefix = _safe_filename(product_title)
        
        dalle_paths = {}
        gemini_paths = {}
        
        prompt_map = {
            "A": prompts.prompt_a,
            "B": prompts.prompt_b,
            "C": prompts.prompt_c
        }
        
        for label, prompt in prompt_map.items():
            if not prompt:
                continue
            
            print(f"  Processing prompt {label}...")
            
            # --- DALL·E image ---
            dalle_name = f"{cat_prefix}__{prod_prefix}__{label}__dalle.png"
            dalle_out = IMAGES_DIR / dalle_name
            dalle_success = _generate_single_dalle_image(OPENAI_CLIENT, prompt, dalle_out)
            dalle_paths[label] = str(dalle_out) if dalle_success else None
            
            # --- Gemini image ---
            gemini_name = f"{cat_prefix}__{prod_prefix}__{label}__gemini.png"
            gemini_out = IMAGES_DIR / gemini_name
            gemini_success = _generate_single_gemini_image(prompt, gemini_out)
            gemini_paths[label] = str(gemini_out) if gemini_success else None
        
        print("[ImageAgent] Image generation completed.")
        return {
            "dalle": dalle_paths,
            "gemini": gemini_paths,
        }


# ----------------------------------------------------------------------
# Orchestrator / Coordinator Agent
# ----------------------------------------------------------------------

class OrchestratorAgent:
    """Coordinates the whole pipeline."""

    def __init__(self, output_dir: str = "outputs"):
        self.product_selector = ProductSelectionAgent()
        self.description_agent = DescriptionAgent()
        self.review_agent = ReviewAgent()
        self.prompt_agent = PromptBuilderAgent()
        self.image_agent = ImageGenerationAgent()
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def _safe_call(self, func, *args, **kwargs):
        """Simple retry wrapper: try once, then retry if exception."""
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"[WARN] Error in {func.__name__}: {e}. Retrying once...")
            return func(*args, **kwargs)

    def run_for_product(self, cfg: ProductConfig) -> ProductRunArtifacts:
        print(f"\\n=== Running agentic workflow for category: {cfg.category} ===")

        # 1) Product selection / metadata
        meta = self._safe_call(self.product_selector.run, cfg)
        print(f"Selected product title: {meta.get('product_title') or meta.get('title')}")

        # 2) Description analysis
        desc = self._safe_call(self.description_agent.run, meta)
        print("Description agent completed.")

        # 3) Review analysis
        reviews = self._safe_call(self.review_agent.run, cfg)
        print(f"Review agent completed on {reviews.n_reviews_sampled} sampled reviews.")

        # 4) Prompt building for image generation
        prompts = self._safe_call(self.prompt_agent.run, cfg, desc, reviews)
        print("Prompt builder agent completed.")
        
        # 5) Image generation using DALL-E and Gemini
        image_output = self._safe_call(self.image_agent.run, cfg.category, 
                                      meta.get('product_title') or meta.get('title') or 'Unknown Product', 
                                      prompts)
        images = ImageOutput(
            dalle_paths=image_output["dalle"],
            gemini_paths=image_output["gemini"]
        )
        print("Image generation agent completed.")

        artifacts = ProductRunArtifacts(
            config=cfg,
            description=desc,
            reviews=reviews,
            prompts=prompts,
            images=images,
        )

        # Save everything to JSON
        self._save_artifacts(artifacts)
        return artifacts

    def _save_artifacts(self, artifacts: ProductRunArtifacts):
        cat = artifacts.config.category
        out = {
            "config": asdict(artifacts.config),
            "description": {
                "raw_description": artifacts.description.raw_description,
                "llm_response": artifacts.description.llm_response,
            },
            "reviews": {
                "n_reviews_sampled": artifacts.reviews.n_reviews_sampled,
                "llm_topics_summary": artifacts.reviews.llm_topics_summary,
            },
            "prompts": {
                "prompt_a": artifacts.prompts.prompt_a,
                "prompt_b": artifacts.prompts.prompt_b,
                "prompt_c": artifacts.prompts.prompt_c,
            },
            "images": {
                "dalle": artifacts.images.dalle_paths,
                "gemini": artifacts.images.gemini_paths,
            },
        }
        path = os.path.join(self.output_dir, f"{cat}_agentic_outputs.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        print(f"Saved artifacts to {path}")


# ----------------------------------------------------------------------
# Main entry point
# ----------------------------------------------------------------------

def main():
    # File paths pointing to out_native folder
    configs = [
        ProductConfig(
            category="Electronics",
            meta_path="out_native/top_product_Electronics_single.csv",
            reviews_path="out_native/cleaned_reviews_Electronics_single.parquet",
        ),
        ProductConfig(
            category="Clothing_Shoes_and_Jewelry",
            meta_path="out_native/top_product_Clothing_Shoes_and_Jewelry_single.csv",
            reviews_path="out_native/cleaned_reviews_Clothing_Shoes_and_Jewelry_single.parquet",
        ),
        ProductConfig(
            category="Health_and_Household",
            meta_path="out_native/top_product_Health_and_Household_single.csv",
            reviews_path="out_native/cleaned_reviews_Health_and_Household_single.parquet",
        ),
    ]

    orchestrator = OrchestratorAgent(output_dir="outputs")

    all_artifacts: List[ProductRunArtifacts] = []
    for cfg in configs:
        artifacts = orchestrator.run_for_product(cfg)
        all_artifacts.append(artifacts)

    print("\\nAgentic workflow complete for all configured products.")
    print(f"Generated images saved to: {IMAGES_DIR}")


if __name__ == "__main__":
    main()