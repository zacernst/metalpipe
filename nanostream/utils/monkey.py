from nanostream.utils.helpers import *


class TreeFinder:
    pass


class MeetsCondition(TreeFinder):

    def __init__(self, test_function=None, direction='down', **kwargs):
        '''
        Fix generator in __call__ so that it depends on whether the argument
        is a generator or a TracedObject.
        '''
        self.test_function = test_function or (lambda x: True)
        self.direction = direction
        super(MeetsCondition, self).__init__(**kwargs)

    def __call__(self, thing):
        generator = (
            thing.descendants if self.direction == 'down' else thing.ancestors)
        if isinstance(self.test_function, (TreeFinder,)):
            out = [
                thing for thing in generator if self.test_function(thing.value)]
        else:
            out = [thing for thing in generator if self.test_function(thing)]
        for one_thing in out:
            yield one_thing


class IsList(MeetsCondition):

    def __init__(self, direction='down', **kwargs):
        super(IsList, self).__init__(
            direction=direction,
            test_function=(lambda x: isinstance(x, (TracedList,))))


class IsDictionary(MeetsCondition):

    def __init__(self, direction='down', **kwargs):
        super(IsList, self).__init__(
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
        'qux': ['foo', 'foobarbaz', 'ding', {'goo': 'blergh'}]}
    thing = splitter(d)
    print(thing['qux'][3].root)
    c = MeetsCondition(
        test_function=(
            lambda x: isinstance(
                x, (TracedPrimitive,)) and x.thing == 'blergh'))
