from typing import Protocol


# Source Protocol is compatible with DataFrame and any object that can be used as a data source.
# DataFrame naturally satisfies this Protocol since it's a structural type.
class Source(Protocol):
    """
    Protocol for data sources in the pipeline.

    This Protocol is satisfied by DataFrame and any object that can be used
    as a data source. DataFrame naturally satisfies this Protocol via duck typing.
    """

    ...
