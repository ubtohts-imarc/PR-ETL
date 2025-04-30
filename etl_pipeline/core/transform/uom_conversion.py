import pandas as pd
from utility.logger import get_logger

logger = get_logger()

class UOMConverter:
    def __init__(self, config: dict):
        """
        Initialize the UOMConverter with a configuration dictionary.
        """
        self.conversion_rates = config.get("conversion_rates", {})
        self.si_units = {
            "days": "mo",
            "area": "m2",
            "energy": "kWh",
            "mass": "kg",
            "volume": "L",
        }

    def validate_input(self, df: pd.DataFrame):
        """
        Validate required columns in DataFrame.
        """
        required_columns = ["price_value", "expected_unit_code", "input_quantity"]
        for column in required_columns:
            if column not in df.columns:
                raise ValueError(f"Missing required column: {column}")

    def convert_to_si_unit(self, unit: str):
        """
        Convert unit to its SI unit.
        """
        for unit_type, units in self.conversion_rates.items():
            if unit and unit.lower() in units:
                if unit_type == "unit":
                    return unit, 1
                si_unit = self.si_units.get(unit_type)
                if si_unit:
                    return si_unit, units[unit.lower()]
        raise ValueError(f"Unit '{unit}' not found in conversion rates.")

    def convert(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert units and compute standardized price.
        """
        self.validate_input(df)

        # Initialize empty columns
        df["final_unit_code"] = None
        df["final_quantity"] = None
        df["si_price_value"] = None

        for index, row in df.iterrows():
            try:
                unit = row["expected_unit_code"]
                quantity = row["input_quantity"]
                price = row["price_value"]

                si_unit, factor = self.convert_to_si_unit(unit)

                # Safe conversion
                final_quantity = quantity / factor
                si_price = price * final_quantity

                df.at[index, "final_unit_code"] = si_unit
                df.at[index, "si_price_value"] = si_price

            except Exception as e:
                logger.warning(f"Conversion failed for row {index} with unit '{row.get('expected_unit_code')}'. Error: {e}")

        # Drop temporary columns if needed
        if "final_quantity" in df.columns:
            df.drop(columns=["final_quantity"], inplace=True, errors="ignore")

        return df