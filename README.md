# Nested Sets Implementation for SqlAlchemy

```python
from sqlalchemy import  String, Integer
from sqlalchemy.orm import declarative_base,  mapped_column, Mapped
from nested_sets import NestedSet

Base = declarative_base()


class Node(Base, NestedSet):
    __tablename__ = "mymodel"
    id : Mapped[int]= mapped_column(Integer(), primary_key=True)
    title : Mapped[str] = mapped_column(String())


node = Node(title='Root')

first_child = Node(title='First Child')
second_child = Node(title='First Child')
    
node.generate_tree()
```