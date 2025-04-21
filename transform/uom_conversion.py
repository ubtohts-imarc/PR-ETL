import pandas as pd
from utility.logger import get_logger

logger = get_logger()

class UOMConverter:
    def __init__(self, config: dict):
        """
        Initialize the UOMConverter with a configuration dictionary.

        Args:
            config (dict): Configuration dictionary containing conversion rates.
        """
        self.conversion_rates = config["conversion_rates"]
        self.si_units = {
            "days": "mo",
            "area": "m2",
            "energy": "kWh",
            "mass": "kg",
            "volume": "L",
        }

    def validate_input(self, df: pd.DataFrame):
        """
        Validate the input DataFrame to ensure required columns are present.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Raises:
            ValueError: If required columns are missing.
        """
        required_columns = ["price_value", "expected_unit_code"]
        for column in required_columns:
            if column not in df.columns:
                raise ValueError(f"Missing required column: {column}")

    def convert_to_si_unit(self, unit: str):
        """
        Convert the input unit to its corresponding SI unit.

        Args:
            unit (str): The Input Quantity Unit.

        Returns:
            tuple: A tuple containing the SI unit and its Conversion Factor.

        Raises:
            ValueError: If the unit is not found in conversion rates.
        """
        for unit_type, units in self.conversion_rates.items():
            if unit.lower() in units:
                # Skip conversion for 'unit' category
                if unit_type == "unit":
                    return unit, 1

                # Use the statically defined SI unit
                si_unit = self.si_units.get(unit_type)
                if si_unit:
                    conversion_factor = units[unit.lower()]
                    return si_unit, conversion_factor

        raise ValueError(f"Unit '{unit}' not found in conversion rates.")

    def convert(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform unit conversion and calculate the final price and quantity.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with additional columns for final quantity unit, final quantity, and final price.
        """
        self.validate_input(df)

        if df["price_value"].isnull().any():
            raise ValueError("Input Price contains non-numeric values or invalid data.")
        
        # Perform conversion
        df["final_unit_code"] = df["expected_unit_code"].apply(
            lambda unit: self.convert_to_si_unit(unit)[0]
        )
        df["final_quantity"] = df["expected_unit_code"].apply(
            lambda unit: self.convert_to_si_unit(unit)[1]
        )

        df["final_quantity"] = df["expected_quantity"] / df["final_quantity"]
        df["si_price_value"] = df["price_value"] * df["final_quantity"]

        # Drop intermediate column
        df = df.drop(columns=["final_quantity"])

        return df