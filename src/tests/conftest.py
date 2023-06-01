import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from tests.node import Base


@pytest.fixture
def engine():

    _engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(_engine)
    return _engine


@pytest.fixture
def session(engine):
    with Session(engine) as session:
        yield session
