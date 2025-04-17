import pandas as pd


class UOMConverter:
    def __init__(self, config: dict):
        """
        Initialize the UOMConverter with a configuration dictionary.

        Args:
            config (dict): Configuration dictionary containing conversion rates.
        """
        self.conversion_rates = config["conversion_rates"]
        self.si_units = {
            "days": "month",
            "energy": "kwh",
            "mass": "kg",
            "volume": "litre",
        }

    def validate_input(self, df: pd.DataFrame):
        """
        Validate the input DataFrame to ensure required columns are present.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Raises:
            ValueError: If required columns are missing.
        """
        required_columns = ["Input Price", "Input Quantity Unit"]
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

        # Ensure 'Input Price' is numeric
        df["Input Price"] = df["Input Price"].astype(str).str.replace("RMB", "").str.replace("USD", "").str.replace("CNY", "").str.replace("¥", "").str.replace("$", "").str.replace(",", "").str.strip()
        df["Input Price"] = pd.to_numeric(df["Input Price"], errors="coerce")
        if df["Input Price"].isnull().any():
            raise ValueError("Input Price contains non-numeric values or invalid data.")


        # Perform conversion
        df["final quantity unit"] = df["Input Quantity Unit"].apply(
            lambda unit: self.convert_to_si_unit(unit)[0]
        )
        df["Conversion Factor"] = df["Input Quantity Unit"].apply(
            lambda unit: self.convert_to_si_unit(unit)[1]
        )
        df["final quantity"] = 1 / df["Conversion Factor"]
        df["final price"] = df["Input Price"] * df["Conversion Factor"]

        # Drop intermediate column
        df = df.drop(columns=["Conversion Factor"])

        return df


# Example usage
if __name__ == "__main__":
    # Example configuration
    config = {
        "conversion_rates": {
            "area": {
                "square meter": 1,
                "acre": 0.0002471053815,
            },
            "days": {
                "month": 1,
                "annum": 12,
            },
            "energy": {
                "kwh": 1,
                "mwh": 0.001,
                "mmbtu": 3412.14,
            },
            "mass": {
                "kg": 1,
                "wet ton": 0.001,
                "mt": 0.001,
                "gram": 1000,
                "troy ounce": 0.0311035,
                "short ton": 0.0011023113109244,
                "lb": 2.20462,
                "candy": 0.003937007,
            },
            "unit": {
                "dozen": 1,
                "gj": 1,
                "1000 Cans": 1,
                "%": 1,
                "1000 Board Feet": 1,
            },
            "volume": {
                "litre": 1,
                "cubic metre": 0.001,
                "%Vol/hl": 0.01,
                "kl": 0.001,
                "gallon": 3.78541,
                "barrel": 0.0062898108,
            },
        },
        "convertible_types": {
            "mass": "volume",
        },
    }

    # Example DataFrame
    data = {
        "Input Price": [100, 200, 300],
        "Input Quantity Unit": ["tone", "gallon", "dozen"],
    }
    df = pd.DataFrame(data)

    # Perform conversion
    converter = UOMConverter(config)
    result_df = converter.convert(df)

    print(result_df)