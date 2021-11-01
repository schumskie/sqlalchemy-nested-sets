import logging
from collections import deque

from sqlalchemy import (Column, Integer, and_, asc, case, desc, event, func,
                        select)
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.orm import (aliased, declarative_mixin, declared_attr, foreign,
                            object_session, relationship, remote)


class NestedSetException(Exception):
    """NestedSetException."""

    pass


class NestedSetMovementNotAllowed(NestedSetException):
    """NestedSetMovementNotAllowed."""

    pass


@declarative_mixin
class NestedSet:
    """NestedSet is a mixin class used for nested set presentation."""

    parent = None

    left = Column("lft", Integer, nullable=False)
    right = Column("rgt", Integer, nullable=False)
    _children = None

    @declared_attr
    def descendants(cls):
        """All descendants of node ordered from left to right.

        :param cls:
        """
        return relationship(
            cls,
            primaryjoin=and_(
                remote(foreign(cls.left)) > cls.left,
                remote(foreign(cls.left)) < cls.right,
            ),
            viewonly=True,
            order_by=remote(foreign(cls.left)),
        )

    @declared_attr
    def ancestors(cls):
        """All ancestors of node ordered from top to bottom.

        :param cls:
        """
        return relationship(
            cls,
            primaryjoin=and_(
                remote(foreign(cls.left)) < cls.left,
                remote(foreign(cls.right)) > cls.right,
            ),
            viewonly=True,
            order_by=remote(foreign(cls.left)),
        )

    @property
    def children(self):
        """Node children. Avaialbe after generate tree is called."""
        if self._children:
            return tuple(self._children)
        return None

    @classmethod
    def __declare_first__(cls):
        """__declare_first__."""
        cls.__mapper__.batch = False

    @classmethod
    def get_primary_key_name(cls):
        """Override this method to your model primary key name. Default is id."""
        return "id"

    @classmethod
    def get_primary_key_column(cls):
        """get_primary_key_column."""
        return getattr(cls, cls.get_primary_key_name())

    @property
    def primary_key(self):
        """primary_key."""
        return getattr(self, self.get_primary_key_name())

    @primary_key.setter
    def primary_key(self, val):
        """primary_key.

        :param val:
        """
        setattr(self, self.get_primary_key_name(), val)

    def __repr__(self):
        """__repr__."""
        return "NestedSet(%s, %d, %d)" % (
            self.primary_key,
            self.left,
            self.right,
        )

    def get_ancestors(self):
        """get_ancestors."""
        session = object_session(self)
        ealias = aliased(self.__class__)
        return (
            session.query(self.__class__)
            .filter(ealias.left.between(self.__class__.left, self.__class__.right))
            .filter(getattr(ealias, self.get_primary_key_name()) == self.primary_key)
            .all()
        )

    def decendents_query(self):
        """decendents_query."""
        session = object_session(self)

        return (
            session.query(self.__class__)
            .filter(self.__class__.left.between(self.left, self.right))
            .order_by(self.__class__.left)
        )

    @hybrid_method
    def is_ancestor_of(self, other, inclusive: bool = False):
        """checks if node is ancestor of target node.

        :param other: target node
        :param inclusive: wheter should include itself
        :type inclusive: bool
        """
        if inclusive:
            return (self.left <= other.left) & (other.right <= self.right)
        return (self.left < other.left) & (other.right < self.right)

    @hybrid_method
    def is_descendant_of(self, other, inclusive: bool = False):
        """Checks if node is descendant of target node.

        :param other: target node
        :param inclusive: wheter should include itself
        :type inclusive: bool
        """
        return other.is_ancestor_of(self, inclusive)

    def generate_tree(self):
        """Generates tree out from node. All descendats get children property availalbe."""
        tree = self
        tree._children = deque()
        stack = [tree]
        for node in self.descendants:
            parent = stack.pop()
            while node.left > parent.right:
                parent = stack.pop()

            node._children = deque()
            parent._children.append(node)
            stack.append(parent)
            stack.append(node)

    def _move_outside(self):
        """_move_outside."""
        table = self.__table__
        session = object_session(self)
        session.execute(
            table.update(
                and_(
                    table.c.lft >= self.left,
                    table.c.rgt <= self.right,
                )
            ).values(lft=-table.c.lft, rgt=-table.c.rgt)
        )

    def _shrink_space(self):
        """_shrink_space."""
        table = self.__table__
        session = object_session(self)
        width = self.right - self.left + 1
        session.execute(
            table.update(and_(table.c.rgt > self.right)).values(
                lft=case(
                    [
                        (
                            table.c.lft > self.right,
                            table.c.lft - width,
                        )
                    ],
                    else_=table.c.lft,
                ),
                rgt=table.c.rgt - width,
            )
        )

    def move_before(self, target):
        """move_before.

        :param target:
        """
        if self.left <= target.left and self.right >= target.right:
            raise NestedSetMovementNotAllowed("Can't move before child node")

        session = object_session(self)
        # Move itself and children outside of tree
        self._move_outside()
        # Shrink unused space
        self._shrink_space()
        # Move other nodes to the right
        move_node_before(self.__table__, session, self, target)

    def move_after(self, target):
        """move_after.

        :param target:
        """
        if self.left <= target.left and self.right >= target.right:
            raise NestedSetMovementNotAllowed("Can't move after child node")
        # self.emp = self.emp
        session = object_session(self)
        # Move itself and children outside of tree
        self._move_outside()
        # Shrink unused space
        self._shrink_space()
        move_node_after(self.__table__, session, self, target)

    def move_inside(self, target):
        """move_inside.

        :param target:
        """
        if self.left <= target.left and self.right >= target.right:
            raise NestedSetMovementNotAllowed("Can't move inside child node")
        session = object_session(self)
        self._move_outside()
        self._shrink_space()
        move_node_inside(self.__table__, session, self, target)


def _primary_key_match(table, instance):
    """primary_key_match.

    :param table:
    :param instance:
    """
    column = instance.get_primary_key_column()
    return getattr(table.c, column.name) == instance.primary_key


@event.listens_for(NestedSet, "before_insert", propagate=True)
def before_insert(mapper, connection, instance):
    """before_insert.

    :param mapper:
    :param connection:
    :param instance:
    """

    nested_sets = mapper.persist_selectable

    if not instance.parent:
        right_most = connection.scalar(
            select(nested_sets.c.rgt).order_by(desc(nested_sets.c.rgt))
        )
        if not right_most:
            right_most = 0
        instance.left = right_most + 1
        instance.right = right_most + 2
    else:
        right_most_sibling = connection.scalar(
            select(nested_sets.c.rgt).where(
                _primary_key_match(nested_sets, instance.parent)
            )
        )

        increase_space(nested_sets, connection, right_most_sibling, 2)
        instance.left = right_most_sibling
        instance.right = right_most_sibling + 1


@event.listens_for(NestedSet, "after_delete", propagate=True)
def after_delete(mapper, connection, instance):
    """after_delete.

    :param mapper:
    :param connection:
    :param instance:
    """
    nested_sets = mapper.persist_selectable
    instance._shrink_space()
    connection.execute(
        nested_sets.delete().where(
            and_(nested_sets.c.lft > instance.left, nested_sets.c.rgt < instance.right)
        )
    )


def increase_space(nested_sets, connection, position, space, inclusive=True):
    """increase_space.

    :param nested_sets:
    :param connection:
    :param position:
    :param space:
    :param inclusive:
    """

    comparason = (
        nested_sets.c.rgt >= position if inclusive else nested_sets.c.rgt > position
    )
    connection.execute(
        nested_sets.update(comparason).values(
            lft=case(
                [
                    (
                        nested_sets.c.lft >= position,
                        nested_sets.c.lft + space,
                    )
                ],
                else_=nested_sets.c.lft,
            ),
            rgt=nested_sets.c.rgt + space,
        )
    )


def return_inside(nested_sets, connection, distance):
    """return_inside.

    :param nested_sets:
    :param connection:
    :param distance:
    """
    connection.execute(
        nested_sets.update(and_(nested_sets.c.lft < 0)).values(
            lft=-nested_sets.c.lft - distance, rgt=-nested_sets.c.rgt - distance
        )
    )


def move_node_before(nested_sets, connection, instance, target):
    """move_node_before.

    :param nested_sets:
    :param connection:
    :param instance:
    :param target:
    """
    width = instance.right - instance.left + 1
    target_left = connection.scalar(
        select(nested_sets.c.lft).where(_primary_key_match(nested_sets, target))
    )
    increase_space(nested_sets, connection, target_left, width)
    distance = instance.left - target_left

    return_inside(nested_sets, connection, distance)


def move_node_after(nested_sets, connection, instance, target):
    """move_node_after.

    :param nested_sets:
    :param connection:
    :param instance:
    :param target:
    """
    width = instance.right - instance.left + 1
    target_right = connection.scalar(
        select(nested_sets.c.rgt).where(_primary_key_match(nested_sets, target))
    )
    increase_space(nested_sets, connection, target_right, width, inclusive=False)
    distance = instance.left - target_right - 1
    return_inside(nested_sets, connection, distance)


def move_node_inside(nested_sets, connection, instance, target):
    """move_node_inside.

    :param nested_sets:
    :param connection:
    :param instance:
    :param target:
    """
    width = instance.right - instance.left + 1
    target_right = connection.scalar(
        select(nested_sets.c.rgt).where(_primary_key_match(nested_sets, target))
    )
    increase_space(nested_sets, connection, target_right, width)
    distance = instance.left - target_right

    return_inside(nested_sets, connection, distance)


def print_tree(session, model):
    """print_tree.

    :param session:
    :param model:
    """
    ealias = aliased(model)
    for indentation, employee in (
        session.query(
            func.count(model.get_primary_key_column()).label("indentation") - 1, ealias
        )
        .filter(ealias.left.between(model.left, model.right))
        .group_by(getattr(ealias, model.get_primary_key_name()))
        .order_by(ealias.left)
    ):
        yield "    " * indentation + repr(employee)
