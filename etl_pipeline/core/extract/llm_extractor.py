from typing import Dict, Any
from pydantic import BaseModel
import json

from groq import Groq

from utility.logger import get_logger

logger = get_logger()

class LLMCommodityInfo(BaseModel):
    weight: str
    price: str

class LLMExtractionOutput(BaseModel):
    default: LLMCommodityInfo
    commodities: Dict[str, LLMCommodityInfo]

client = Groq(api_key="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")

def render_prompt(template: str, schema: dict, input_text: str) -> str:
    """Render prompt using simple string replacement"""
    return (
        template.replace("{{schema}}", json.dumps(schema, indent=2))
                .replace("{{input}}", input_text.strip())
    )

def extract_units(remarks: str, config: dict) -> LLMExtractionOutput:
    """Generic LLM extractor for commodity units"""

    prompt_template = config["prompt_template"]
    model = config.get("model", config["model"])
    response_format = config.get("response_format", {"type": "json_object"})

    schema = LLMExtractionOutput.model_json_schema()
    prompt = render_prompt(prompt_template, schema, remarks)

    logger.info(f"Prompt: {prompt}")

    completion = client.chat.completions.create(
        model=model,
        response_format=response_format,
        stream=False,
        temperature=0,
        messages=[
            {"role": "system", "content": prompt}
        ]
    )

    return LLMExtractionOutput.model_validate_json(completion.choices[0].message.content)
