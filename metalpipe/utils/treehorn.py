import sys
import pprint
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
        self._retrieve_key = []
        self._next_traversal = None
        self._retrieval_dict = {}
        super(GoSomewhere, self).__init__(**kwargs)

    def __getitem__(self, key):
        self._retrieve_keys.append(key)
        return self

    def update_retrieval_dict(self, key=None, value=None, r_dict=None):
        if r_dict is not None:
            self._retrieval_dict.update(r_dict)
        elif key is not None and value is not None:
            self._retrieval_dict[key] = value
        else:
            raise Exception('Need either a key/value or a dictionary.')


    @property
    def head(self):
        start = self
        while start._previous_traversal is not None:
            start = start._previous_traversal
        return start

    @property
    def tail(self):
        end = self
        while end._next_traversal is not None:
            end = end._next_traversal
        return end

    def all_traversals(self):
        head = self.head
        chain = [head]
        current = head
        while current._next_traversal is not None:
            chain.append(current._next_traversal)
            current = current._next_traversal
        return chain

    def __call__(self, thing, result_list=None):
        result_list = result_list or {}

        if not isinstance(thing, (TracedObject,)):
            thing = splitter(thing)

        start_traversal = self

        def _nodes_meeting_condition(_traversal, _tree):
            if _traversal.direction == "down":
                _inner_generator = _tree.descendants
            elif _traversal.direction == "up":
                _inner_generator = _tree.ancestors
            elif _traversal.direction == "here":
                _inner_generator = _tree.this
            else:
                raise Exception("This should definitely not happen.")

            for inner_node in _inner_generator():
                _traversal._current_result = inner_node
                if (
                    _traversal.condition(inner_node)
                    == _traversal.condition.truth_value
                ):
                    yield inner_node

        for node in _nodes_meeting_condition(self, thing):
            result_list[self.label] = node
            if self._next_traversal is None:
                yield result_list
            else:
                for i in self._next_traversal(node, result_list=result_list):
                    yield i
                    result_list = {}


    def matches(self, thing):
        self(thing)
        for i in self._generator:
            yield i

    def next_traversal(self, go_somewhere):
        tail_traversal = self
        while tail_traversal._next_traversal is not None:
            tail_traversal = tail_traversal._next_traversal
        tail_traversal._next_traversal = go_somewhere
        go_somewhere._previous_traversal = tail_traversal

    def __getattr__(self, thing):
        raise Exception("not implemented..." + thing)

    def apply_label(self, label):
        self.label = label
        return self

    def __add__(self, label):
        self.label = label
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
        else:
            raise Exception('This should not be happening, either.')

    def __repr__(self):
        out = "{class_name}[{condition}]: {label}".format(
            class_name=self.__class__.__name__,
            condition=str(self.condition.__class__.__name__),
            label=str(self.label),
        )
        return out


class KeyPath:
    def __init__(self, label=None, keypath=None, traversal_label=None):
        self.keypath = keypath or []
        self.label = label
        self.traversal_label = traversal_label

    def split_label(self):
        self.label = self.keypath[0]
        self.keypath = self.keypath[1:]

    def __repr__(self):
        out = '{label} : {keypath}'.format(label=str(self.label), keypath=str(self.keypath))
        return out


class GoDown(GoSomewhere):
    def __init__(self, **kwargs):
        self.direction = "down"
        super(GoDown, self).__init__(**kwargs)


class GoUp(GoSomewhere):
    def __init__(self, **kwargs):
        self.direction = "up"
        super(GoUp, self).__init__(**kwargs)


class StayHere(GoSomewhere):
    def __init__(self, **kwargs):
        self.direction = "here"
        super(StayHere, self).__init__(condition=Yes(), **kwargs)


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


class IsRoot(MeetsCondition):
    def __init__(self, **kwargs):
        def _condition(thing):
            return thing.is_root

        super(IsRoot, self).__init__(condition=_condition, **kwargs)


class HasKey(MeetsCondition):
    def __init__(self, key=None, **kwargs):
        self.key = key

        def _condition(thing):
            return isinstance(thing, (TracedDictionary,)) and self.key in thing

        super(HasKey, self).__init__(test_function=_condition, **kwargs)

    def __repr__(self):
        out = 'HasKey({key})'.format(key=str(key))
        return out


class Yes(MeetsCondition):
    def __init__(self, **kwargs):
        def _condition(thing):
            return True

        super(Yes, self).__init__(test_function=_condition, **kwargs)


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
        """
        Test for equality by running ``to_python`` on ``self`` and/or ``other`` first, if necessary.
        """
        return (self.to_python() if hasattr(self, "to_python") else self) == (
            other.to_python() if hasattr(other, "to_python") else other
        )

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

    def this(self):
        yield self

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

    def get(self, keypath):
        obj = self
        for one_key in keypath:
            try:
                obj = obj[one_key]
            except:
                print('fail')
                import pdb; pdb.set_trace()
        return obj


class Relation:
    def __init__(self, name):
        self.name = name
        self.traversal = None
        # Too clever by half
        # globals()[name] = self

    def __eq__(self, traversal):
        self.traversal = traversal
        return self

    def __call__(self, tree):
        if not isinstance(tree, (TracedObject,)):
            tree = splitter(tree)
        traversal_head = self.traversal.head
        # traversal_head(tree)
        traversals = traversal_head.all_traversals()
        traversal_dict = {
            traversal.label: traversal for traversal in traversal_head.all_traversals() if traversal.label is not None
                }
        for i in traversal_head(tree):
            out = {}
            for traversal_name, traversal in traversal_dict.items():
                for key, keypath in traversal._retrieval_dict.items():
                    out[key] = i[traversal_name].get(tuple(keypath))
            yield out

    def __repr__(self):
        out = 'Relation: {name}\nTraversal: {traversal}'.format(name=self.name, traversal=str(self.traversal))
        return out


if __name__ == "__main__":
    SAMPLE_FILE = "/home/vagrant/github/metalpipe/tests/sample_data/sample_treehorn_1.json"

    with open(SAMPLE_FILE, "r") as infile:
        tree = json.load(infile)

    # tree = splitter(tree)

    has_email_key = GoDown(condition=HasKey("email"))
    has_city_key = GoDown(condition=HasKey("city"))
    stick = StayHere()

    has_email_key + 'profile'
    has_city_key + 'city'
    email_retrieval_dict = {'email_address': ['email'], 'name': ['username']}
    address_retrieval_dict = {'latitude': ['geo', 'lat'], 'longitude': ['geo', 'lng']}
    #address_retrieval_dict = {'city': ['zipcode']}
    has_email_key.update_retrieval_dict(r_dict=email_retrieval_dict)
    has_city_key.update_retrieval_dict(r_dict=address_retrieval_dict)

    # Put the retrieval dict in the corresponding Traversal

    from_city = Relation("FROM_CITY")
    from_city == has_email_key > has_city_key
    # sys.exit(0)
    for email_city in from_city(tree):
        print('-------')
        pprint.pprint(email_city)
