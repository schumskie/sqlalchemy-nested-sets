import pytest
from nested_sets import NestedSet, print_tree
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

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


Node = namedtuple("Node", ["title", "children"])


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

    return {
        "albert": albert,
        "bert": bert,
        "chuck": chuck,
        "donna": donna,
        "eddie": eddie,
        "fred": fred,
    }


def test_multiple_tree_initiation(session):
    node1 = MyModel(title="Node1")
    node1_child = MyModel(title="Node1Child", parent=node1)
    node2 = MyModel(title="Node2")
    node3 = MyModel(title="Node3")
    session.add_all([node1, node2, node3, node1_child])
    session.commit()
    print()
    for line in print_tree(session, MyModel):
        print(line)


def test_single_tree(session):
    albert = MyModel(title="Albert")
    bert = MyModel(title="Bert", parent=albert)
    chuck = MyModel(title="Chuck", parent=albert)
    donna = MyModel(title="Donna", parent=chuck)
    eddie = MyModel(title="Eddie", parent=chuck)
    fred = MyModel(title="Fred", parent=chuck)

    session.add_all([albert, bert, chuck, donna, eddie, fred])
    session.commit()
    assert bert in albert.descendants
    assert chuck in albert.descendants
    assert donna in chuck.descendants
    assert eddie in chuck.descendants
    assert fred in chuck.descendants
    print()
    for line in print_tree(session, MyModel):
        print(line)


def test_move_before(session):
    node1 = MyModel(title="Node1")
    node1_child = MyModel(title="Node1Child", parent=node1)
    node2 = MyModel(title="Node2")
    node3 = MyModel(title="Node3")
    session.add_all([node1, node2, node3, node1_child])
    session.commit()
    node3.move_before(node1)
    session.commit()
    print()
    for line in print_tree(session, MyModel):
        print(line)


def test_tree(session):
    root = MyModel(title="Root")
    child_first = MyModel(title="First Child", parent=root)
    child_second = MyModel(title="Second Child", parent=root)
    print("Parent", child_second.parent)
    session.add_all([root, child_first, child_second])
    session.commit()
    print(root.descendants)
    # child_first.parent = root
    # child_second = MyModel(title="Second Child", parent=root)
    # child_second.parent = root
    # grand_child = MyModel(title="Grand Child", parent=root)
    # grand_child.parent = child_second
    # child_second.parent = root
    # session.add_all([root, child_first, child_second, grand_child])
    # session.add(child_first)
    # session.add(child_second)

    # session.commit()
    # print("Descendants")
    # for node in child_second.descendants:
    #    print(node)

    # print("Ancestors")
    # for node in grand_child.ancestors:
    #    print(node)

    # for line in print_tree(session, MyModel):
    #    print(line)
    # print(root.drilldown_tree())
    # for line in print_tree(session, MyModel):
    #    print(line)
