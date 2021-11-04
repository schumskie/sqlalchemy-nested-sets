from collections import namedtuple

import pytest
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy_nested_sets import NestedSet

Base = declarative_base()


class MyModel(Base, NestedSet):
    __tablename__ = "mymodel"
    title = Column(String, primary_key=True)

    @classmethod
    def get_primary_key_name(cls):
        return "title"

    def __repr__(self):
        return f"<TestModel(title={self.title} l={self.left} r={self.right})>"


@pytest.fixture
def engine():

    _engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(_engine)
    yield _engine
    _engine.dispose()


@pytest.fixture
def session(engine):
    return sessionmaker(bind=engine)()


Node = namedtuple("Node", ["title", "children"], defaults=(None, None))


def generate_node(model):
    return Node(
        title=model.title,
        children=tuple(generate_node(child) for child in model.children)
        if model.children
        else None,
    )


def model_to_tree(model):
    model.generate_tree()
    return generate_node(model)


def select_by_title(session, title):
    return session.query(MyModel).where(MyModel.title == title).one()


@pytest.fixture
def output_tree():
    return Node(
        title="Albert",
        children=(
            Node(title="Bert"),
            Node(
                title="Chuck",
                children=(Node(title="Donna"), Node(title="Eddie"), Node(title="Fred")),
            ),
        ),
    )


@pytest.fixture
def base_tree(session):
    albert = MyModel(title="Albert")
    bert = MyModel(title="Bert", parent=albert)
    chuck = MyModel(title="Chuck", parent=albert)
    donna = MyModel(title="Donna", parent=chuck)
    eddie = MyModel(title="Eddie", parent=chuck)
    fred = MyModel(title="Fred", parent=chuck)

    session.add_all([albert, bert, chuck, donna, eddie, fred])
    session.commit()

    return albert


def test_single_tree(session, base_tree, output_tree):
    assert model_to_tree(base_tree) == output_tree


def test_move_before(session, base_tree):
    eddie = select_by_title(session, "Eddie")
    donna = select_by_title(session, "Donna")
    eddie.move_before(donna)
    session.commit()
    assert eddie.right + 1 == donna.left


def test_move_after(session, base_tree):
    eddie = select_by_title(session, "Eddie")
    donna = select_by_title(session, "Donna")
    donna.move_after(eddie)
    session.commit()
    assert eddie.right + 1 == donna.left


def test_move_inside(session, base_tree):
    eddie = select_by_title(session, "Eddie")
    donna = select_by_title(session, "Donna")
    donna.move_inside(eddie)
    session.commit()
    assert eddie.right == donna.right + 1
    assert eddie.left == donna.left - 1
