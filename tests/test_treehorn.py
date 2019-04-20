import os
import logging
import json
import time
import pytest
import metalpipe.utils.treehorn as treehorn


os.environ["PYTHONPATH"] = "."
SAMPLE_DATA_DIR = 'tests/sample_data/'

logging.basicConfig(level=logging.INFO)


def assert_generates(generator, iterable):
    iterable = list(iterable)  # not set because items might not be hashable
    generated_elements = list(generator)
    assert sorted(iterable) == sorted(generated_elements)


def test_assert_generates_true():
    assert_generates(range(3), [0, 1, 2])


def test_assert_generates_true_wrong_order():
    assert_generates(range(3), [1, 2, 0])


def test_assert_generates_false_too_many():
    with pytest.raises(expected_exception=Exception):
        assert_generates(range(3), [1, 2, 0, "bar"])


def test_assert_generates_false_not_enough():
    with pytest.raises(expected_exception=Exception):
        assert_generates(range(3), [1, 2])

@pytest.fixture(scope='function')
def sample_json_dict():
    with open(SAMPLE_DATA_DIR + 'sample_treehorn_1.json') as f:
        d = json.load(f)
    return d



@pytest.fixture(scope="function")
def sample_dictionary():
    d = {
        "bar": "bar",
        "bar": "baz",
        "baz": {"barbar": 1, "goober": 2},
        "qux": ["bar", "barbarbaz", "ding", {"goo": "blergh"}],
        "a": {"b": {"c": "d", "some_list": [1, 2, 4]}},
        "a1": {"b1": {"c1": "d1", "some_list": [10, 20, 40], "e": "whatever"}},
    }
    return d


@pytest.fixture(scope="function")
def sample_traced_object(sample_dictionary):
    return treehorn.splitter(sample_dictionary)


def test_use_splitter_to_instantiate(sample_dictionary):
    obj = treehorn.splitter(sample_dictionary)
    assert isinstance(obj, (treehorn.TracedObject,))


def test_splitter_generates_traced_dictionary(sample_traced_object):
    assert isinstance(sample_traced_object, (treehorn.TracedDictionary,))


def test_splitter_generates_traced_primitive(sample_traced_object):
    obj = sample_traced_object["bar"]
    assert isinstance(obj, (treehorn.TracedPrimitive,))


def test_splitter_generates_traced_list(sample_traced_object):
    obj = sample_traced_object["qux"]
    assert isinstance(obj, (treehorn.TracedList,))


def test_find_root(sample_traced_object):
    assert sample_traced_object["qux"][3]["goo"].root is sample_traced_object


def test_has_descendant_dictionary(sample_traced_object):
    obj = treehorn.GoDown(condition=treehorn.HasDescendant(treehorn.IsDictionary()))
    obj(sample_traced_object)
    result = list(obj._generator)
    assert sample_traced_object["qux"] in result
    assert sample_traced_object["a1"] in result
    assert sample_traced_object["a"] in result
    assert len(result) == 3
    print(result)


def test_not_list(sample_traced_object):
    obj = treehorn.GoDown(condition=treehorn.Not(treehorn.IsList()))
    obj(sample_traced_object)
    result = list(obj._generator)
    objects_in = [
        sample_traced_object["bar"],
        sample_traced_object["bar"],
        sample_traced_object["baz"],
        sample_traced_object["a"],
        sample_traced_object["a1"],
        sample_traced_object["a1"]["b1"],
        sample_traced_object["a1"]["b1"]["c1"],
        sample_traced_object["a1"]["b1"]["e"],
    ]

    objects_out = [
        sample_traced_object["qux"],
        sample_traced_object["a1"]["b1"]["some_list"],
    ]

    for obj in objects_in:
        assert obj in result
    for obj in objects_out:
        assert obj not in result


def test_and(sample_traced_object):
    obj = treehorn.GoDown(condition=treehorn.HasKey("c1") & treehorn.HasKey("e"))
    obj(sample_traced_object)
    result = list(obj._generator)
    assert sample_traced_object["a1"]["b1"] in result
    assert len(result) == 1


def test_traced_object_equality():
    d = {"bar": ["bar", "baz"], "qux": 0}
    assert treehorn.splitter(d) == treehorn.splitter(d)


def test_traced_object_not_equal():
    d1 = {"bar": ["bar", "baz"], "qux": 0}
    d2 = {"bar": ["bar", "baz"], "qux": 1}
    assert treehorn.splitter(d1) != treehorn.splitter(d2)


def test_list_index_equal():
    assert treehorn.ListIndex(1) == treehorn.ListIndex(1)


def test_list_index_not_equal():
    assert treehorn.ListIndex(1) != treehorn.ListIndex(2)


def test_instantiate_downward_traversal():
    obj = treehorn.GoDown()
    assert isinstance(obj, (treehorn.GoDown,))


def test_instantiate_condition():
    obj = treehorn.HasKey("key")
    assert isinstance(obj, (treehorn.HasKey,))


def test_traversal_creates_generator(sample_traced_object):
    obj = treehorn.GoDown(condition=treehorn.HasKey("a1"))
    obj(sample_traced_object)
    assert obj._generator is not None


def test_has_key_finds_node(sample_traced_object):
    obj = treehorn.GoDown(condition=treehorn.HasKey("c1"))
    obj(sample_traced_object)
    result = list(obj._generator)
    assert result[0] == treehorn.splitter(
        {"c1": "d1", "some_list": [10, 20, 40], "e": "whatever"}
    )
    assert len(result) == 1


def test_has_key_correctly_finds_nothing(sample_traced_object):
    obj = treehorn.GoDown(condition=treehorn.HasKey("nonexistent"))
    obj(sample_traced_object)
    result = list(obj._generator)
    assert len(result) == 0


def test_has_key_in_conjunction_finds_node(sample_traced_object):
    obj = treehorn.GoDown(condition=treehorn.HasKey("c1") & treehorn.HasKey("e"))
    obj(sample_traced_object)
    result = list(obj._generator)
    assert result[0] == treehorn.splitter(
        {"c1": "d1", "some_list": [10, 20, 40], "e": "whatever"}
    )
    assert len(result) == 1


def test_has_key_in_disjunction_finds_node(sample_traced_object):
    obj = treehorn.GoDown(
        condition=treehorn.HasKey("c1") | treehorn.HasKey("nonexistent")
    )
    obj(sample_traced_object)
    result = list(obj._generator)
    assert result[0] == treehorn.splitter(
        {"c1": "d1", "some_list": [10, 20, 40], "e": "whatever"}
    )
    assert len(result) == 1


def test_has_key_in_disjunction_negation_finds_node(sample_traced_object):
    obj = treehorn.GoDown(
        condition=treehorn.HasKey("c1") & (~treehorn.HasKey("nonexistent"))
    )
    obj(sample_traced_object)
    result = list(obj._generator)
    assert result[0] == treehorn.splitter(
        {"c1": "d1", "some_list": [10, 20, 40], "e": "whatever"}
    )
    assert len(result) == 1


@pytest.fixture(scope="function")
def email_condition():
    has_email_key = treehorn.GoDown(condition=treehorn.HasKey('email'))
    return has_email_key


@pytest.fixture(scope="function")
def city_condition():
    has_city_key = treehorn.GoDown(condition=treehorn.HasKey('city'))
    has_city_key + 'city'
    return has_city_key


@pytest.fixture(scope='function')
def sample_relation():
    test_relation = treehorn.Relation('TEST_RELATION')
    return test_relation


def test_add_label_to_traversal(email_condition):
    email_condition + 'email'
    assert hasattr(email_condition, 'label')
    assert isinstance(email_condition.label, (treehorn.Label,))


def test_instantiate_relation(email_condition, sample_relation):
    assert isinstance(sample_relation, (treehorn.Relation,))


def test_link_traverals(email_condition, city_condition):
    email_condition > city_condition
    assert email_condition._next_traversal is city_condition
    assert city_condition._previous_traversal is email_condition

def test_iterate_relation(sample_json_dict):
    assert True
