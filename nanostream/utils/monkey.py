from nanostream.utils.helpers import *


class TreeFinder:
    pass


class MeetsCondition(TreeFinder):

    def __init__(self, test_function=None, **kwargs):
        self.test_function = test_function or (lambda x: True)
        super(MeetsCondition, self).__init__(**kwargs)

    def __call__(self, thing):
        return self.test_function(thing)


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
        self.parent_list_index = parent_list_index


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
            self.append(
                splitter(
                    item,
                    path=self.path + [ListIndex(index)],
                    parent=self,
                    parent_key=None,
                    parent_list_index=ListIndex(index)))

    def __repr__(self):
        return [i.__repr__() for i in self].__repr__()


def splitter(
    thing,
    path=None,
    parent=None,
    parent_key=None,
        parent_list_index=None):

    if isinstance(thing, (dict,)):
        return TracedDictionary(thing, path=path, parent=parent, parent_key=parent_key)
    elif isinstance(thing, (list, tuple,)):
        return TracedList(*thing, path=path, parent=parent, parent_key=parent_key)
    else:
        return TracedPrimitive(thing, path=path, parent=parent, parent_key=parent_key)


class TracedPrimitive(TracedObject):

    def __init__(self, thing, path=None, parent=None, parent_key=None):
        self.thing = thing
        super(TracedPrimitive, self).__init__(path=path, parent=parent, parent_key=parent_key)

    def __repr__(self):
        return self.thing.__repr__()


class TracedDictionary(TracedObject, dict):

    def __init__(
        self,
        thing,
        path=None,
        parent=None,
        parent_key=None,
            parent_list_index=None):
        self.thing = thing
        super(TracedDictionary, self).__init__(path=path, parent=parent, parent_key=parent_key)
        for key, value in thing.items():
            self[key] = splitter(
                value, parent=self, parent_key=key, path=self.path + [key])

    def __repr__(self):
        return self.thing.__repr__()


if __name__ == '__main__':
    d = {
        'foo': 'bar',
        'bar': 'baz',
        'baz': {'foobar': 1, 'goober': 2},
        'qux': ['foo', 'foobarbaz', 'ding', {'goo': 'blergh'}]}
    thing = splitter(d)

