from pydantic import BaseModel
from typing import Optional


# Esquema de entrada (datos originales del dataset customers)
class InputPersonaSchema(BaseModel):
    customer_id: int
    first_name: str
    last_name: str
    email: str
    age: Optional[int]


# Esquema de salida (datos procesados)
class OutputPersonaSchema(BaseModel):
    customer_id: int
    full_name: str
    email: str
    age: Optional[int]