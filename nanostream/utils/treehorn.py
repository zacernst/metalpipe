import types
from nanostream.utils.helpers import *


class TreeFinder:
    pass


class MeetsCondition(TreeFinder):

    def __init__(self, test_function=None, truth_value=True, direction='down', **kwargs):
        '''
        Fix generator in __call__ so that it depends on whether the argument
        is a generator or a TracedObject.
        '''
        self.test_function = test_function or (lambda x: True)
        self.direction = direction
        self.truth_value = truth_value
        super(MeetsCondition, self).__init__(**kwargs)

    def _test(self, node):
        return self.test_function(node.value)

    @staticmethod
    def _trivial_generator(thing):
        yield thing

    def __call__(self, thing):
        thing = MeetsCondition._trivial_generator(thing) if not isinstance(
            thing, (types.GeneratorType,)) else thing
        for outer_node in thing:
            generator = (
                outer_node.descendants if self.direction == 'down'
                else outer_node.ancestors)
            for node in generator:
                if self.test_function(node) == self.truth_value:
                    yield node

    def __and__(self, other):
        return And(self, other)

    def __invert__(self):
        return Not(self)

    def __or__(self, other):
        return ~ (~ self & ~ other)

    def __eq__(self, other):
        return (self & other) | (~ self & ~ other)

    def __ne__(self, other):
        return ~ (self == other)


class HasDescendantOrAncestor(MeetsCondition):

    def __init__(self, condition_object=None, **kwargs):
        self.condition_object = condition_object
        super(HasDescendantOrAncestor, self).__init__(**kwargs)

    def __call__(self, thing):
        generator = (
            thing.descendants if isinstance(self, (HasDescendant,)) else
            thing.ancestors)
        for outer_node in generator:
            sentinal = False
            for _ in self.condition_object(outer_node):
                sentinal = True
                break
            if sentinal:
                yield outer_node


class HasDescendant(HasDescendantOrAncestor):

    def __init__(self, condition_object=None, **kwargs):
        super(HasDescendant, self).__init__(
            condition_object=condition_object, **kwargs)


class HasAncestor(HasDescendantOrAncestor):

    def __init__(self, condition_object=None, **kwargs):
        super(HasAncestor, self).__init__(
            condition_object=condition_object, **kwagrs)


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
            return (
                self.conjunct_1.test_function(node) and
                self.conjunct_2.test_function(node))

        super(And, self).__init__(test_function=_test_function, **kwargs)


class Not(MeetsCondition):

    def __init__(self, condition, **kwargs):
        self.condition = condition

        def _test_function(node):
            return not self.condition.test_function(node)

        super(Not, self).__init__(test_function=_test_function, **kwargs)


class IsList(MeetsCondition):

    def __init__(self, direction='down', **kwargs):
        super(IsList, self).__init__(
            test_function=(lambda x: isinstance(x, (TracedList,))), **kwargs)


class IsDictionary(MeetsCondition):

    def __init__(self, direction='down', **kwargs):
        super(IsDictionary, self).__init__(
            direction=direction,
            test_function=(lambda x: isinstance(x, (TracedDictionary,))))


class FromKey(TreeFinder):
    pass


class TracedObject:

    def __init__(
        self,
        path=None,
        parent=None,
        parent_key=None,
            parent_list_index=None):
        self.path = path or []
        self.parent = parent
        self.parent_key = parent_key
        self.children = []
        self.parent_list_index = parent_list_index

    @property
    def is_root(self):
        return self.parent is None

    @property
    def is_leaf(self):
        return

    @property
    def ancestors(self):
        if self.parent is not None:
            yield self.parent
            for i in  self.parent.ancestors:
                yield i

    @property
    def descendants(self):
        for child in self.children:
            yield child
            for grandchild in child.descendants:
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
        return 'ListIndex({index})'.format(index=self.index)


class TracedList(TracedObject, list):

    def __init__(
        self,
        *args,
        path=None,
        parent=None,
        parent_key=None,
            parent_list_index=None):

        super(TracedList, self).__init__(
            path=path,
            parent=parent,
            parent_key=parent_key,
            parent_list_index=parent_list_index)
        for index, item in enumerate(args):
            child = splitter(
                item,
                path=self.path + [ListIndex(index)],
                parent=self,
                parent_key=None,
                parent_list_index=ListIndex(index))
            self.children.append(child)
            self.append(child)

    def __repr__(self):
        return ', '.join([item.__repr__() for item in self])


def splitter(
    thing,
    path=None,
    parent=None,
    parent_key=None,
        parent_list_index=None):

    if isinstance(thing, (dict,)):
        return TracedDictionary(
            thing, path=path, parent=parent, parent_key=parent_key)
    elif isinstance(thing, (list, tuple,)):
        return TracedList(
            *thing, path=path, parent=parent, parent_key=parent_key)
    else:
        return TracedPrimitive(
            thing, path=path, parent=parent, parent_key=parent_key)


class TracedPrimitive(TracedObject):

    def __init__(self, thing, path=None, parent=None, parent_key=None):

        self.thing = thing
        super(TracedPrimitive, self).__init__(
            path=path, parent=parent, parent_key=parent_key)


class TracedDictionary(TracedObject, dict):

    def __init__(
        self,
        thing,
        path=None,
        parent=None,
        parent_key=None,
            parent_list_index=None):

        self.thing = thing
        super(
            TracedDictionary, self).__init__(
                path=path, parent=parent, parent_key=parent_key)
        for key, value in thing.items():
            child = splitter(
                value, parent=self, parent_key=key, path=self.path + [key])
            self[key] = child
            self.children.append(child)


if __name__ == '__main__':
    d = {
        'foo': 'bar',
        'bar': 'baz',
        'baz': {'foobar': 1, 'goober': 2},
        'qux': ['foo', 'foobarbaz', 'ding', {'goo': 'blergh'}],
        'a': {'b': {'c': 'd', 'some_list': [1, 2, 4,]}},
        'a1': {'b1': {'c': 'd', 'some_list': [1, 2, 4,], 'e': 'whatever'}}}
    thing = splitter(d)
    print(thing['qux'][3].root)
    c = MeetsCondition(
        test_function=(
            lambda x: isinstance(
                x, (TracedPrimitive,)) and x.thing == 'blergh'))
    print(list(c(thing)))
    c = IsList(thing)
    print(list(c(thing)))
    c = HasKey(key='goo')
    print(list(c(thing)))
    has_goober = HasKey(key='goober')
    has_foobar = HasKey(key='foobar')
    conjunction = And(has_goober, has_foobar)
    print(list(conjunction(thing)))
    not_list = Not(IsList())
    print(list(not_list(thing)))
    d_test = HasDescendant(IsDictionary())
    print('---')
    print(list(d_test(thing)))
