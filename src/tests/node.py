from collections import namedtuple

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, declarative_base

from nested_sets import NestedSet

Base = declarative_base()


class Node(Base, NestedSet):
    __tablename__ = "node"
    title: Mapped[str] = mapped_column(String(), primary_key=True)

    @classmethod
    def get_primary_key_name(cls):
        return "title"

    def __repr__(self):
        return f"<TestModel(title={self.title} l={self.left} r={self.right})>"


NodeTuple = namedtuple("Node", ["title", "children"], defaults=(None, None))


def generate_node(model: Node):
    return NodeTuple(
        title=model.title,
        children=tuple(generate_node(child) for child in model.children)
        if model.children
        else None,
    )


def model_to_tree(model):
    model.generate_tree()
    return generate_node(model)
