import pytest
from tests.node import Node, model_to_tree, NodeTuple


def select_by_title(session, title):
    return session.query(Node).where(Node.title == title).one()


@pytest.fixture
def output_tree():
    return NodeTuple(
        title="Albert",
        children=(
            NodeTuple(title="Bert"),
            NodeTuple(
                title="Chuck",
                children=(NodeTuple(title="Donna"), NodeTuple(title="Eddie"), NodeTuple(title="Fred")),
            ),
        ),
    )


@pytest.fixture
def base_tree(session):
    albert = Node(title="Albert")
    bert = Node(title="Bert", parent=albert)
    chuck = Node(title="Chuck", parent=albert)
    donna = Node(title="Donna", parent=chuck)
    eddie = Node(title="Eddie", parent=chuck)
    fred = Node(title="Fred", parent=chuck)

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
