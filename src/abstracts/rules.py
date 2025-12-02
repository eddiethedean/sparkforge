from typing import Protocol


# Rules Protocol is compatible with ColumnRules (Dict[str, List[Union[str, Column]]])
# Any dictionary mapping column names to rule lists satisfies this Protocol.
class Rules(Protocol):
    """
    Protocol for validation rules.

    This Protocol is satisfied by ColumnRules (Dict[str, List[Union[str, Column]]])
    and any dictionary mapping column names to rule lists.
    """

    ...
