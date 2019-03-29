import json
import logging
import types
import hashlib
from metalpipe.utils.helpers import *


# logging.basicConfig(level=logging.INFO)


def cast_generators(some_dict):
    if isinstance(some_dict, (dict,)):
        for key, value in some_dict.items():
            if isinstance(value, (types.GeneratorType,)):
                some_dict[key] = list(value)


class TreeHorn:
    pass


class Label:
    def __init__(self, label):
        self.label = label
        self.treehorns = []

    def __le__(self, other):
        if isinstance(other, (TreeHorn,)):
            self.treehorns.append(other)
        else:
            raise Exception("Right side of Label assignment must be TreeHorn.")

    def __repr__(self):
        return "Label({label})".format(label=self.label)

    def __eq__(self, other):
        return self.label == other.label

    def __hash__(self):
        return int(hashlib.md5(bytes(self.label, "utf8")).hexdigest(), 16)


class GoSomewhere(TreeHorn, dict):
    def __init__(self, condition=None, **kwargs):
        self.condition = condition
        self.label = None
        self._current_result = None
        self._previous_traversal = None
        self._generator = None
        self._retrieve_key = None
        self._next_traversal = None
        super(GoSomewhere, self).__init__(**kwargs)

    def __getitem__(self, key):
        self._retrieve_key = key
        return self

    @property
    def head(self):
        start = self
        while start._previous_traversal is not None:
            start = start._previous_traversal
        return start

    def all_traversals(self):
        head = self.head
        chain = [head]
        current = head
        while current._next_traversal is not None:
            chain.append(current._next_traversal)
            current = current._next_traversal
        return chain

    def __call__(self, thing):
        # Find start of traversal

        if not isinstance(thing, (TracedObject,)):
            thing = splitter(thing)

        start_traversal = self

        def _generator(_traversal, _tree):
            _inner_generator = (
                _tree.descendants
                if _traversal.direction == "down"
                else _tree.ancestors
            )

            for inner_node in _inner_generator():
                _traversal._current_result = inner_node
                if (
                    _traversal.condition(inner_node)
                    == _traversal.condition.truth_value
                ):
                    if _traversal._next_traversal is None:
                        _traversal._current_result = inner_node
                        yield inner_node
                    else:
                        _traversal._next_traversal(inner_node)
                        for result in _traversal._next_traversal._generator:
                            _traversal._next_traversal._current_result = result
                            yield result

        self._generator = _generator(start_traversal, thing)

    def matches(self, thing):
        self(thing)
        for i in self._generator:
            yield i

    def next_traversal(self, go_somewhere):
        self._next_traversal = go_somewhere
        go_somewhere._previous_traversal = self

    def __getattr__(self, thing):

        raise Exception("not implemented..." + thing)

    def apply_label(self, label):
        self.label = label
        return self

    def __add__(self, label):
        self.label = Label(label)
        return self

    def __gt__(self, other):
        self.next_traversal(other)
        return self

    def __iter__(self):
        for i in self._generator:
            yield i

    def get(self, key_or_keys):
        if isinstance(key_or_keys, (str,)):
            return self.get(key)
        elif isinstance(key_or_keys, (list, tuple)):
            return [self.get(key) for key in key_or_keys]


class GoDown(GoSomewhere):
    def __init__(self, **kwargs):
        self.direction = "down"
        super(GoDown, self).__init__(**kwargs)


class GoUp(GoSomewhere):
    def __init__(self, **kwargs):
        self.direction = "up"
        super(GoDown, self).__init__(**kwargs)


class MeetsCondition(TreeHorn):
    def __init__(self, test_function=None, truth_value=True, **kwargs):
        """
        Fix generator in __call__ so that it depends on whether the argument
        is a generator or a TracedObject.
        """
        self.test_function = test_function or (lambda x: True)
        self.truth_value = truth_value

    def _test(self, node):
        return self.test_function(node.value)

    @staticmethod
    def _trivial_generator(thing):
        yield thing

    def __call__(self, thing):
        return self.test_function(thing)

    def __and__(self, other):
        return And(self, other)

    def __invert__(self):
        return Not(self)

    def __or__(self, other):
        return ~(~self & ~other)

    def __eq__(self, other):
        return (self & other) | (~self & ~other)

    def __ne__(self, other):
        return ~(self == other)


class HasDescendantOrAncestor(MeetsCondition):
    def __init__(self, condition=None, **kwargs):
        self.condition = condition
        super(HasDescendantOrAncestor, self).__init__(**kwargs)

    def __call__(self, thing):
        generator = (
            thing.descendants()
            if isinstance(self, (HasDescendant,))
            else thing.ancestors()
        )
        for outer_node in generator:
            sentinal = False
            for _ in self.condition(outer_node):
                sentinal = True
                break
            if sentinal:
                yield outer_node


class HasDescendant(HasDescendantOrAncestor):
    def __init__(self, condition=None, **kwargs):
        self.condition = condition
        super(HasDescendant, self).__init__(condition=condition, **kwargs)

    def __call__(self, thing):
        return any(self.condition(node) for node in thing.descendants())


class HasAncestor(HasDescendantOrAncestor):
    def __init__(self, condition=None, **kwargs):
        self.condition = condition
        super(HasAncestor, self).__init__(condition=condition, **kwagrs)

    def __call__(self, thing):
        return any(self.condition(node) for node in thing.descendants())


class HasKey(MeetsCondition):
    def __init__(self, key=None, **kwargs):
        self.key = key

        def _condition(thing):
            return isinstance(thing, (TracedDictionary,)) and self.key in thing

        super(HasKey, self).__init__(test_function=_condition, **kwargs)


class And(MeetsCondition):
    def __init__(self, conjunct_1, conjunct_2, **kwargs):
        self.conjunct_1 = conjunct_1
        self.conjunct_2 = conjunct_2

        def _test_function(node):
            return self.conjunct_1.test_function(
                node
            ) and self.conjunct_2.test_function(node)

        super(And, self).__init__(test_function=_test_function, **kwargs)


class Not(MeetsCondition):
    def __init__(self, condition, **kwargs):
        self.condition = condition

        def _test_function(node):
            return not self.condition.test_function(node)

        super(Not, self).__init__(test_function=_test_function, **kwargs)


class IsList(MeetsCondition):
    def __init__(self, direction="down", **kwargs):
        super(IsList, self).__init__(
            test_function=(lambda x: isinstance(x, (TracedList,))), **kwargs
        )


class IsDictionary(MeetsCondition):
    def __init__(self, direction="down", **kwargs):
        super(IsDictionary, self).__init__(
            direction=direction,
            test_function=(lambda x: isinstance(x, (TracedDictionary,))),
        )


class TracedObject:
    def __init__(
        self, path=None, parent=None, parent_key=None, parent_list_index=None
    ):
        self.path = path or []
        self.parent = parent
        self.parent_key = parent_key
        self.labels = set()
        self.children = []
        self.parent_list_index = parent_list_index

    def enumerate(self):
        return self.root.descendants()

    def __eq__(self, other):
        return self.to_python() == other.to_python()

    def to_python(self):
        """
        Disfrobulates the object back into Python primitive types.
        """
        if isinstance(self, (TracedDictionary,)):
            out = {key: value.to_python() for key, value in self.items()}
        elif isinstance(self, (TracedList,)):
            out = [i.to_python() for i in self]
        elif isinstance(self, (TracedPrimitive,)):
            out = self.thing
        return out

    @property
    def is_root(self):
        return self.parent is None

    @property
    def is_leaf(self):
        return

    def ancestors(self, include_self=False):
        if include_self:
            yield self
        if self.parent is not None:
            yield self.parent
            for i in self.parent.ancestors(include_self=False):
                yield i

    def descendants(self, include_self=False):
        if include_self:
            yield self
        for child in self.children:
            yield child
            for grandchild in child.descendants(include_self=False):
                yield grandchild

    @property
    def root(self):
        if self.is_root:
            return self
        else:
            return self.parent.root

    def __repr__(self):
        return self.thing.__repr__()


class ListIndex:
    def __init__(self, index):
        self.index = index

    def __repr__(self):
        return "ListIndex({index})".format(index=self.index)

    def __eq__(self, other):
        return self.index == other.index


class TracedList(TracedObject, list):
    def __init__(
        self,
        *args,
        path=None,
        parent=None,
        parent_key=None,
        parent_list_index=None
    ):

        super(TracedList, self).__init__(
            path=path,
            parent=parent,
            parent_key=parent_key,
            parent_list_index=parent_list_index,
        )
        for index, item in enumerate(args):
            child = splitter(
                item,
                path=self.path + [ListIndex(index)],
                parent=self,
                parent_key=None,
                parent_list_index=ListIndex(index),
            )
            self.children.append(child)
            self.append(child)

    def __repr__(self):
        return "[" + ", ".join([item.__repr__() for item in self]) + "]"


class PathEndsIn(MeetsCondition):
    def __init__(self, path=None, **kwargs):
        self.path = path or []
        _test_function = lambda x: x.path[-1 * len(self.path) :] == self.path
        super(PathEndsIn, self).__init__(
            test_function=_test_function, **kwargs
        )


def splitter(
    thing, path=None, parent=None, parent_key=None, parent_list_index=None
):

    if isinstance(thing, (dict,)):
        return TracedDictionary(
            thing, path=path, parent=parent, parent_key=parent_key
        )
    elif isinstance(thing, (list, tuple)):
        return TracedList(
            *thing, path=path, parent=parent, parent_key=parent_key
        )
    else:
        return TracedPrimitive(
            thing, path=path, parent=parent, parent_key=parent_key
        )


class TracedPrimitive(TracedObject):
    def __init__(self, thing, path=None, parent=None, parent_key=None):

        self.thing = thing
        super(TracedPrimitive, self).__init__(
            path=path, parent=parent, parent_key=parent_key
        )


class TracedDictionary(TracedObject, dict):
    def __init__(
        self,
        thing,
        path=None,
        parent=None,
        parent_key=None,
        parent_list_index=None,
    ):

        self.thing = thing
        super(TracedDictionary, self).__init__(
            path=path, parent=parent, parent_key=parent_key
        )
        for key, value in thing.items():
            child = splitter(
                value, parent=self, parent_key=key, path=self.path + [key]
            )
            self[key] = child
            self.children.append(child)


class Relation:
    def __init__(self, name):
        self.name = name
        globals()[name] = self

    def __eq__(self, traversal):
        self.traversal = traversal
        return self

    def __call__(self, tree):
        if not isinstance(tree, (TracedObject,)):
            tree = splitter(tree)
        traversal_head = self.traversal.head
        traversal_head(tree)
        traversals = traversal_head.all_traversals()
        with_labels = [
            traversal
            for traversal in traversals
            if traversal.label is not None
        ]
        traversal_head(tree)
        for _ in traversal_head._generator:
            d = {}
            for node_with_label in with_labels:
                d[node_with_label.label.label] = (
                    node_with_label._current_result
                    if node_with_label._retrieve_key is None
                    else node_with_label._current_result.get(
                        node_with_label._retrieve_key
                    )
                )
            yield d


if __name__ == "__main__":
    SAMPLE_FILE = "/home/zac/projects/metalpipe/sample_output.json"

    with open(SAMPLE_FILE, "r") as infile:
        tree = json.load(infile)

    # tree = splitter(tree)

    has_email_key = GoDown(condition=HasKey("email"))
    has_city_key = GoDown(condition=HasKey("city"))

    for i in has_email_key.matches(tree):
        print(i)

    Relation("FROM_CITY") == (
        (has_email_key + "email")["email"] > (has_city_key + "city")["city"]
    )
    for email_city in FROM_CITY(tree):
        pass
        print(email_city)
