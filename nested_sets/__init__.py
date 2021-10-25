import logging
from sqlalchemy import case, and_
from sqlalchemy import Column
from sqlalchemy import event
from sqlalchemy import Integer
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.orm.collections import collection
from sqlalchemy.orm import (
    object_session,
    aliased,
    relationship,
    foreign,
    remote,
    declared_attr,
    declarative_mixin,
)
from sqlalchemy import select, func, asc, desc


@declarative_mixin
class NestedSet:

    parent = None

    left = Column("lft", Integer, nullable=False)
    right = Column("rgt", Integer, nullable=False)

    @declared_attr
    def descendants(cls):
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
        last_right = 0
        result = []
        for node in self.descendants:
            if node.left > last_right:
                result.append(node)
                last_right = node.right
        return result

    @classmethod
    def __declare_first__(cls):
        cls.__mapper__.batch = False

    @classmethod
    def get_primary_key_name(cls):
        return "id"

    @classmethod
    def get_primary_key_column(cls):
        return getattr(cls, cls.get_primary_key_name())

    @property
    def primary_key(self):
        return getattr(self, self.get_primary_key_name())

    @primary_key.setter
    def primary_key(self, val):
        setattr(self, self.get_primary_key_name(), val)

    def __repr__(self):
        return "NestedSet(%s, %d, %d)" % (
            self.primary_key,
            self.left,
            self.right,
        )

    def get_ancestors(self):
        session = object_session(self)
        ealias = aliased(self.__class__)
        return (
            session.query(self.__class__)
            .filter(ealias.left.between(self.__class__.left, self.__class__.right))
            .filter(getattr(ealias, self.get_primary_key_name()) == self.primary_key)
            .all()
        )

    def decendents_query(self):
        session = object_session(self)

        return (
            session.query(self.__class__)
            .filter(self.__class__.left.between(self.left, self.right))
            .order_by(self.__class__.left)
        )

    @hybrid_method
    def is_ancestor_of(self, other, inclusive=False):
        """class or instance level method which returns True if self is
        ancestor (closer to root) of other else False. Optional flag
        `inclusive` on whether or not to treat self as ancestor of self.
        For example see:
        * :mod:`sqlalchemy_mptt.tests.cases.integrity.test_hierarchy_structure`
        """
        if inclusive:
            return (self.left <= other.left) & (other.right <= self.right)
        return (self.left < other.left) & (other.right < self.right)

    @hybrid_method
    def is_descendant_of(self, other, inclusive=False):
        """class or instance level method which returns True if self is
        descendant (farther from root) of other else False.  Optional flag
        `inclusive` on whether or not to treat self as descendant of self.
        For example see:
        * :mod:`sqlalchemy_mptt.tests.cases.integrity.test_hierarchy_structure`
        """
        return other.is_ancestor_of(self, inclusive)

    @classmethod
    def _base_query(cls, session=None):
        return session.query(cls)

    def _base_query_obj(self, session=None):
        if not session:
            session = object_session(self)
        return self._base_query(session)

    @classmethod
    def _base_order(cls, query, order=asc):
        return query.order_by(order(cls.left))

    def path_to_root(self, session=None, order=desc):
        table = self.__class__
        query = self._base_query_obj(session=session)
        query = query.filter(table.is_ancestor_of(self, inclusive=True))
        return self._base_order(query, order=order)

    def drilldown_tree(self):

        tree = {"node": self, "children": []}
        stack = [tree]
        for node in self.descendants:
            parent = stack.pop()
            while node.left > parent["node"].right:
                parent = stack.pop()

            n = {"node": node, "children": []}
            parent["children"].append(n)
            stack.append(parent)
            stack.append(n)

        return tree

    def _move_outside(self):
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
        if self.left <= target.left and self.right >= target.right:
            raise Exception("Can't move before child node")

        session = object_session(self)
        # Move itself and children outside of tree
        self._move_outside()
        # Shrink unused space
        self._shrink_space()
        # Move other nodes to the right
        move_node_before(self.__table__, session, self, target)

    def move_after(self, target):
        if self.left <= target.left and self.right >= target.right:
            raise Exception("Can't move after child node")
        # self.emp = self.emp
        session = object_session(self)
        # Move itself and children outside of tree
        self._move_outside()
        # Shrink unused space
        self._shrink_space()
        move_node_after(self.__table__, session, self, target)

    def move_inside(self, target):
        if self.left <= target.left and self.right >= target.right:
            raise Exception("Can't move inside child node")
        session = object_session(self)
        self._move_outside()
        self._shrink_space()
        move_node_inside(self.__table__, session, self, target)


def primary_key_match(table, instance):
    column = instance.get_primary_key_column()
    return getattr(table.c, column.name) == instance.primary_key


@event.listens_for(NestedSet, "before_insert", propagate=True)
def before_insert(mapper, connection, instance):

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
                primary_key_match(nested_sets, instance.parent)
            )
        )

        increase_space(nested_sets, connection, right_most_sibling, 2)
        instance.left = right_most_sibling
        instance.right = right_most_sibling + 1


@event.listens_for(NestedSet, "after_delete", propagate=True)
def after_delete(mapper, connection, instance):
    nested_sets = mapper.persist_selectable
    shrink_space(nested_sets, connection, instance)
    connection.execute(
        nested_sets.delete().where(
            and_(nested_sets.c.lft > instance.left, nested_sets.c.rgt < instance.right)
        )
    )


def shrink_space(nested_sets, connection, instance):
    width = instance.right - instance.left + 1
    connection.execute(
        nested_sets.update(and_(nested_sets.c.rgt > instance.right)).values(
            lft=case(
                [
                    (
                        nested_sets.c.lft > instance.right,
                        nested_sets.c.lft - width,
                    )
                ],
                else_=nested_sets.c.lft,
            ),
            rgt=nested_sets.c.rgt - width,
        )
    )


def increase_space(nested_sets, connection, position, space, inclusive=True):

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


def move_outside(nested_sets, connection, instance):
    connection.execute(
        nested_sets.update(
            and_(
                nested_sets.c.lft >= instance.left,
                nested_sets.c.rgt <= instance.right,
            )
        ).values(lft=-nested_sets.c.lft, rgt=-nested_sets.c.rgt)
    )


def return_inside(nested_sets, connection, distance):
    connection.execute(
        nested_sets.update(and_(nested_sets.c.lft < 0)).values(
            lft=-nested_sets.c.lft - distance, rgt=-nested_sets.c.rgt - distance
        )
    )


def move_node_before(nested_sets, connection, instance, target):
    width = instance.right - instance.left + 1
    target_left = connection.scalar(
        select(nested_sets.c.lft).where(primary_key_match(nested_sets, target))
    )
    increase_space(nested_sets, connection, target_left, width)
    distance = instance.left - target_left

    return_inside(nested_sets, connection, distance)


def move_node_after(nested_sets, connection, instance, target):
    width = instance.right - instance.left + 1
    target_right = connection.scalar(
        select(nested_sets.c.rgt).where(primary_key_match(nested_sets, target))
    )
    increase_space(nested_sets, connection, target_right, width, inclusive=False)
    distance = instance.left - target_right - 1
    return_inside(nested_sets, connection, distance)


def move_node_inside(nested_sets, connection, instance, target):
    width = instance.right - instance.left + 1
    target_right = connection.scalar(
        select(nested_sets.c.rgt).where(primary_key_match(nested_sets, target))
    )
    increase_space(nested_sets, connection, target_right, width)
    distance = instance.left - target_right

    return_inside(nested_sets, connection, distance)


def print_tree(session, model):
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
