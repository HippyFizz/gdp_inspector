from typing import NewType, Union, Dict, Optional, List

FieldTypes = NewType('FieldTypes', Union[str, int, float])
RowType = NewType('RowType', Dict[str, FieldTypes])
DbFormat = NewType('DbFormat', Optional[Dict[str, Union[List[RowType], RowType]]])
