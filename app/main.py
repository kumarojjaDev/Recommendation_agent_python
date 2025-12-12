# main.py
import os
import json
import logging
from typing import List, Optional, Dict, Any, Set, Tuple
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.database_repository import get_all_products, find_by_exact_or_partial_name
from app import config
from models.schemas import Product, PublicProduct, RecommendationRequest, RecommendationResponse

from google import genai
from google.genai import types
import google.genai.errors as genai_errors

# ==========================
# Config & Client
# ==========================
GEMINI_API_KEY = config.GEMINI_API_KEY or os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY environment variable is not set")

GEMINI_MODEL = config.GEMINI_MODEL
GEMINI_FALLBACK_MODEL = config.GEMINI_FALLBACK_MODEL

client = genai.Client(api_key=GEMINI_API_KEY)

app = FastAPI(
    title="Recommendation Agent API",
    version="1.0.0",
    description="AI-powered product recommendation agent using Gemini + configurable data source (JSON/MongoDB).",
)

logger = logging.getLogger("recommendation_agent")
logging.basicConfig(level=logging.INFO)

# ==========================
# Compatibility map & helpers
# ==========================
PLACEHOLDER_BRANDS = {"generic", "unknown", "n/a", "", None}

# Conservative compatibility map. Add categories deliberately.
COMPATIBILITY_MAP: Dict[str, Set[str]] = {
    "phone": {"phone", "phone_case", "screen_guard", "charger", "pouch", "earbuds", "power_bank"},
    "watch_strap": {"watch_strap", "watch_tool", "spring_bar", "watch_box"},
    "charger": {"cable", "adapter", "power_bank"},
    "printer": {"ink_cartridge", "toner", "print_head", "maintenance_kit", "paper"},
    "tv": {"soundbar", "home_theatre", "remote", "wall_mount"},
    "pan": {"scrubber", "spatula", "lid", "pan_care_kit"},
    "nebulizer": {"mask", "filters", "tubing", "mouthpiece"},
    # VERY IMPORTANT: medical_equipment should only match medical accessories/consumables.
    "medical_equipment": {"medical_equipment", "medical_supplies", "medical_accessory", "filters", "mask", "tubing", "mouthpiece"},
    # Be conservative for speakers: prefer direct speaker accessories only (no chargers/power banks unless explicit)
    "speaker": {"speaker", "speaker_stand", "aux_cable", "bluetooth_transmitter", "case"},
    # extend with your categories...
}


def is_meaningful_brand(brand: Optional[str]) -> bool:
    if brand is None:
        return False
    return str(brand).strip().lower() not in PLACEHOLDER_BRANDS


def tag_overlap_count(a_tags: Optional[List[str]], b_tags: Optional[List[str]]) -> int:
    if not a_tags or not b_tags:
        return 0
    return len(set(a_tags).intersection(set(b_tags)))


def attribute_match_score(primary: Product, candidate: Product) -> int:
    """Domain-specific attribute scoring. Extend for new categories as needed."""
    score = 0
    # Phone strong match
    if primary.category == "phone":
        if candidate.category in ("phone_case", "screen_guard", "pouch", "charger"):
            if candidate.attributes.get("compatible_model") and candidate.attributes.get("compatible_brand"):
                if (candidate.attributes.get("compatible_model") == primary.model
                        and candidate.attributes.get("compatible_brand") == primary.brand):
                    score += 50
            if candidate.category == "charger":
                if candidate.attributes.get("port_type") == primary.attributes.get("port_type"):
                    score += 25

    # Watch strap
    if primary.category == "watch_strap":
        if candidate.attributes.get("size_mm") and primary.attributes.get("size_mm"):
            if candidate.attributes.get("size_mm") == primary.attributes.get("size_mm"):
                score += 40

    # Printer series/brand
    if primary.category == "printer":
        if candidate.category in ("ink_cartridge", "toner", "print_head"):
            if (candidate.attributes.get("compatible_series") and primary.attributes.get("series")
               and candidate.attributes.get("compatible_series") == primary.attributes.get("series")):
                score += 50
            if candidate.attributes.get("compatible_brand") and candidate.attributes.get("compatible_brand") == primary.brand:
                score += 20

    # Nebulizer compatibility
    if primary.category == "nebulizer":
        if candidate.category in ("mask", "filters", "tubing"):
            if candidate.attributes.get("compatible_model") == primary.model or candidate.attributes.get("compatible_brand") == primary.brand:
                score += 40

    # Speaker: prefer accessories explicitly tagged as speaker accessories or with explicit compatibility
    if primary.category == "speaker":
        if candidate.attributes.get("compatible_with_speaker") or ("speaker" in (candidate.tags or [])):
            score += 20

    # Medical equipment: high score for explicit medical compatibility fields
    if primary.category == "medical_equipment":
        if candidate.category in ("medical_supplies", "medical_accessory", "filters", "mask", "tubing", "mouthpiece"):
            if candidate.attributes.get("compatible_model") and candidate.attributes.get("compatible_brand"):
                if (candidate.attributes.get("compatible_model") == primary.model
                        or candidate.attributes.get("compatible_brand") == primary.brand):
                    score += 80
            # consumables that explicitly list the compatible device or brand
            if candidate.attributes.get("compatible_with_medical_model") == primary.model:
                score += 60
            # matching tags
            if tag_overlap_count(primary.tags, candidate.tags) > 0:
                score += 10

    return score


def to_public_product(prod: Product) -> PublicProduct:
    return PublicProduct(
        id=prod.id,
        name=prod.name,
        category=prod.category,
        brand=prod.brand,
        model=prod.model,
    )


# ==========================
# Agents
# ==========================
class RetrieverAgent:
    """Responsible for fetching raw products from your data source."""
    def retrieve(self, primary: Product, max_results: int = 500) -> List[Product]:
        products = get_all_products()
        prioritized: List[Product] = []
        rest: List[Product] = []
        for p in products:
            if p.id == primary.id:
                continue
            # Strong preference for same-category items
            if p.category == primary.category:
                prioritized.append(p)
                continue
            # If primary has a meaningful brand, prioritize same-brand accessories (but not over same-category)
            if is_meaningful_brand(primary.brand) and is_meaningful_brand(p.brand) and p.brand == primary.brand:
                prioritized.append(p)
                continue
            # For sensitive categories (medical_equipment), deprioritize unrelated consumer electronics
            if primary.category == "medical_equipment" and p.category in ("phone", "charger", "speaker", "phone_case", "screen_guard"):
                rest.append(p)  # push to back
                continue
            rest.append(p)
        results = prioritized + rest
        return results[:max_results]


class CandidateBuilderAgent:
    """Prunes by category compatibility and applies conservative inclusion rules when no map exists."""
    def __init__(self, compat_map: Dict[str, Set[str]] = COMPATIBILITY_MAP):
        self.compat_map = compat_map

    def build(self, primary: Product, raw_candidates: List[Product]) -> List[Product]:
        allowed = self.compat_map.get(primary.category)
        out: List[Product] = []
        for p in raw_candidates:
            if p.id == primary.id:
                continue

            # If explicit compatibility map exists, enforce category membership
            if allowed is not None:
                if p.category not in allowed:
                    continue

                # If candidate is cross-category, require explicit compatibility attributes or at least 2 tag overlaps
                if p.category != primary.category:
                    tag_overlap = tag_overlap_count(primary.tags, p.tags)
                    has_explicit_attr = bool(
                        p.attributes.get("compatible_model")
                        or p.attributes.get("compatible_brand")
                        or p.attributes.get("compatible_with")
                        or p.attributes.get(f"compatible_with_{primary.category}")
                        or p.attributes.get("compatible_with_speaker")
                        or p.attributes.get("compatible_with_medical_model")
                    )
                    if not has_explicit_attr and tag_overlap < 2:
                        continue
            else:
                # No compatibility map entry — be conservative
                tag_overlap = tag_overlap_count(primary.tags, p.tags)
                has_explicit_attr = bool(
                    p.attributes.get("compatible_model")
                    or p.attributes.get("compatible_brand")
                    or p.attributes.get("compatible_with")
                    or p.attributes.get(f"compatible_with_{primary.category}")
                    or p.attributes.get("compatible_with_speaker")
                    or p.attributes.get("compatible_with_medical_model")
                )
                if tag_overlap < 2 and not has_explicit_attr:
                    continue

            # Example explicit exclusion: don't recommend phone screen guards for watch straps
            if primary.category == "watch_strap" and p.category == "screen_guard":
                continue

            out.append(p)
        return out


class ScorerAgent:
    """Deterministic scoring used for shortlisting and fallback."""
    def score(self, primary: Product, candidates: List[Product]) -> List[Product]:
        scored: List[Product] = []
        for c in candidates:
            score = 0
            score += attribute_match_score(primary, c)
            if is_meaningful_brand(primary.brand) and is_meaningful_brand(c.brand) and primary.brand == c.brand:
                score += 30
            tag_overlap = tag_overlap_count(primary.tags, c.tags)
            score += 10 * tag_overlap
            if c.category in COMPATIBILITY_MAP.get(primary.category, set()):
                score += 20
            if c.model_dump().get("image_url"):
                score += 2
            setattr(c, "_reco_score", score)
            setattr(c, "_reco_tag_overlap", tag_overlap)
            scored.append(c)
        scored.sort(key=lambda x: getattr(x, "_reco_score", 0), reverse=True)
        return scored


class LLMReRankerAgent:
    """Uses Gemini to re-rank a provided candidate shortlist."""
    def __init__(self, client: genai.Client, primary_model: Optional[str], fallback_model: Optional[str]):
        self.client = client
        self.primary_model = primary_model
        self.fallback_model = fallback_model

    def rerank(self, primary: Product, candidates: List[Product], limit: int) -> Tuple[Optional[List[int]], Dict[str, str]]:
        if not self.primary_model and not self.fallback_model:
            return None, {}

        primary_dict = primary.model_dump()
        candidates_dict = [c.model_dump() for c in candidates]
        allowed = COMPATIBILITY_MAP.get(primary.category)
        allowed_list = sorted(list(allowed)) if allowed else []

        prompt = f"""
You are a product recommendation re-ranker.

Primary product:
{json.dumps(primary_dict, ensure_ascii=False)}

Candidate products (THE ONLY PRODUCTS YOU MAY CHOOSE FROM):
{json.dumps(candidates_dict, ensure_ascii=False)}

Allowed candidate categories for this primary:
{json.dumps(allowed_list, ensure_ascii=False)}

Important:
- You MUST choose only from the candidate products provided above.
- You MUST NOT select product categories that are not in the allowed list for this primary.
- If there is no allowed list for this primary, only select candidates that have explicit compatibility attributes (compatible_model, compatible_brand, compatible_with_<category>, compatible_with_speaker, compatible_with_medical_model, etc).
- Return ONLY valid JSON in the exact format described below.

Return format (JSON ONLY):
{{
  "primary_item_id": <number>,
  "recommendation_ids": [<number>, ...],   # up to {limit}
  "reasons": {{ "<id>": "<short reason>", ... }}   # optional
}}
"""
        models_to_try = [self.primary_model, self.fallback_model]
        response = None
        for model_name in models_to_try:
            if not model_name:
                continue
            try:
                response = self.client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.2,
                        max_output_tokens=512,
                    ),
                )
                break
            except genai_errors.ClientError as exc:
                logger.warning("Gemini client model %s failed: %s", model_name, exc)
                response = None
                continue

        if response is None:
            logger.info("LLM re-rank: no model responded")
            return None, {}

        try:
            parsed = json.loads(response.text)
        except json.JSONDecodeError:
            logger.warning("Gemini returned invalid JSON: %s", getattr(response, "text", None))
            return None, {}

        rec_ids = parsed.get("recommendation_ids", [])
        if not isinstance(rec_ids, list):
            logger.warning("LLM returned bad recommendation_ids type")
            return None, {}

        candidate_ids = {c.id for c in candidates}
        clean_ids: List[int] = []
        for rid in rec_ids:
            try:
                rid_int = int(rid)
            except (TypeError, ValueError):
                continue
            if rid_int in candidate_ids and rid_int not in clean_ids:
                clean_ids.append(rid_int)
            else:
                logger.debug("LLM recommended id %s not in candidate set or duplicate", rid)
        if not clean_ids:
            return None, {}

        reasons = parsed.get("reasons", {}) if isinstance(parsed.get("reasons", {}), dict) else {}
        return clean_ids[:limit], reasons


class ValidatorAgent:
    """Enforces business rules and safety checks on the final picks."""
    def validate(self, primary: Product, rec_ids: List[int], candidates: List[Product]) -> List[int]:
        id_to_c = {c.id: c for c in candidates}
        valid: List[int] = []
        allowed = COMPATIBILITY_MAP.get(primary.category)
        for rid in rec_ids:
            c = id_to_c.get(rid)
            if not c:
                continue

            # If allowed map exists, candidate category must be in it
            if allowed is not None and c.category not in allowed:
                logger.info("Validator blocked candidate %s with category %s not allowed for primary %s", rid, c.category, primary.id)
                continue

            # Special: do not recommend phones for speakers unless explicit compatibility exists
            if primary.category == "speaker" and c.category == "phone":
                if not (c.attributes.get("compatible_with_speaker") or c.attributes.get("compatible_with") or c.attributes.get("cross_compatible_with_speaker")):
                    logger.info("Validator blocked phone %s for speaker primary %s", rid, primary.id)
                    continue

            # Special: for speakers, only allow chargers/power_banks when explicitly compatible
            if primary.category == "speaker" and c.category in ("charger", "power_bank"):
                explicit_ok = bool(
                    c.attributes.get("compatible_with_speaker")
                    or c.attributes.get("compatible_with")
                    or c.attributes.get("cross_compatible_with_speaker")
                )
                power_match = False
                speaker_req = primary.attributes.get("required_voltage") or primary.attributes.get("required_input")
                charger_out = c.attributes.get("output_voltage") or c.attributes.get("output")
                if speaker_req and charger_out and str(speaker_req) == str(charger_out):
                    power_match = True
                if not (explicit_ok or power_match):
                    logger.info("Validator blocked charger/power_bank %s for speaker %s (no explicit compatibility)", rid, primary.id)
                    continue

            # Critical: for medical_equipment, block unrelated consumer-electronics unless explicit medical compatibility
            if primary.category == "medical_equipment":
                # permit only items in medical categories or those explicitly declaring medical compatibility
                if c.category not in ("medical_supplies", "medical_accessory", "filters", "mask", "tubing", "mouthpiece", "medical_equipment"):
                    # allow only if candidate explicitly claims compatibility with this medical device
                    explicit_med_ok = bool(
                        c.attributes.get("compatible_with_medical_model")
                        or c.attributes.get("compatible_brand") == primary.brand
                        or c.attributes.get("compatible_model") == primary.model
                    )
                    if not explicit_med_ok:
                        logger.info("Validator blocked non-medical candidate %s for medical primary %s", rid, primary.id)
                        continue

            # Printer consumable safety: do not recommend other-brand cartridges unless cross_compatible flag present
            if primary.category == "printer" and c.category in ("ink_cartridge", "toner"):
                comp_brand = c.attributes.get("compatible_brand")
                if comp_brand and comp_brand != primary.brand:
                    if not c.attributes.get("cross_compatible", False):
                        logger.info("Validator blocked incompatible cartridge %s for printer %s", rid, primary.id)
                        continue

            valid.append(rid)
        return valid


class OrchestratorAgent:
    def __init__(self, client: genai.Client, primary_model: Optional[str], fallback_model: Optional[str]):
        self.retriever = RetrieverAgent()
        self.builder = CandidateBuilderAgent()
        self.scorer = ScorerAgent()
        self.reranker = LLMReRankerAgent(client, primary_model, fallback_model)
        self.validator = ValidatorAgent()

    def recommend(self, primary: Product, limit: int = 5) -> List[Dict[str, Any]]:
        # 1) retrieve raw candidates
        raw_candidates = self.retriever.retrieve(primary, max_results=1000)
        logger.debug("Retrieved %d raw candidates for primary %s", len(raw_candidates), primary.id)

        # 2) build candidates (category-first pruning / conservative rules)
        candidates = self.builder.build(primary, raw_candidates)
        logger.info("CandidateBuilder produced %d candidates for primary %s (category=%s)", len(candidates), primary.id, primary.category)
        if not candidates:
            logger.info("No candidates after CandidateBuilder for primary %s", primary.id)
            return []

        # 3) deterministic score & short-list
        scored = self.scorer.score(primary, candidates)
        top_m_limit = 30
        top_m = scored[:top_m_limit]
        logger.debug("Top-%d candidates after scoring: %s", top_m_limit, [(c.id, getattr(c, "_reco_score", 0)) for c in top_m])

        # 4) try LLM re-rank
        rec_ids, reasons = self.reranker.rerank(primary, top_m, limit)

        # fallback deterministic if LLM failed
        if not rec_ids:
            logger.info("LLM failed or returned nothing — falling back to deterministic top-N")
            rec_ids = [c.id for c in top_m[:limit]]
            reasons = {}

        # 5) validate
        valid_ids = self.validator.validate(primary, rec_ids, top_m)

        # If validator filtered everything, fallback again to deterministic top-n (but only same-category)
        if not valid_ids:
            logger.info("Validator filtered all picks; falling back to deterministic top-N limited to same-category")
            same_cat = [c.id for c in top_m if c.category == primary.category]
            valid_ids = same_cat[:limit] or [c.id for c in top_m[:limit]]

        # 6) Build ordered output
        id_to_p = {p.id: p for p in top_m}
        recommendations: List[Dict[str, Any]] = []
        for rid in valid_ids[:limit]:
            p = id_to_p.get(rid)
            if not p:
                p = next((s for s in scored if s.id == rid), None)
            if not p:
                continue
            public = to_public_product(p)
            recommendations.append({"product": public, "reason": reasons.get(str(rid)) or reasons.get(rid) or None})
        return recommendations


# ==========================
# Orchestrator instance
# ==========================
orchestrator = OrchestratorAgent(client, GEMINI_MODEL, GEMINI_FALLBACK_MODEL)


# ==========================
# API endpoints
# ==========================
@app.get("/health")
def health_check():
    """Health check endpoint to verify service status."""
    return {
        "status": "healthy",
        "data_source": "mongodb" if config.USE_MONGO else "json",
        "gemini_model": config.GEMINI_MODEL
    }


@app.post("/recommendations", response_model=RecommendationResponse)
def recommend(request: RecommendationRequest) -> RecommendationResponse:
    # 1) find primary
    primary = find_by_exact_or_partial_name(request.item_name)
    if not primary:
        return RecommendationResponse(primary_item=None, recommendations=[])

    # 2) orchestrated recommendation
    recs = orchestrator.recommend(primary, limit=request.limit)

    rec_products: List[PublicProduct] = []
    for r in recs:
        prod: PublicProduct = r.get("product")
        if prod:
            rec_products.append(prod)

    return RecommendationResponse(
        primary_item=to_public_product(primary),
        recommendations=rec_products,
    )
