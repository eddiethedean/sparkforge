from typing import Protocol


# Transformer Protocol is compatible with SilverTransformFunction and GoldTransformFunction
# Any callable that transforms data satisfies this Protocol.
class Transformer(Protocol):
    """
    Protocol for transformation functions.

    This Protocol is satisfied by:
    - SilverTransformFunction: Callable[[SparkSession, DataFrame, Dict[str, DataFrame]], DataFrame]
    - GoldTransformFunction: Callable[[SparkSession, Dict[str, DataFrame]], DataFrame]
    - Any callable that transforms data sources
    """

    ...
