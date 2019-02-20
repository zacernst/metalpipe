========
Treehorn
========

Treehorn is a set of classes for manipulating dictionary- and list-like objects in a declarative style. It is meant to be useful for the sort of tasks required for ETL, such as extracting structured data from JSON objects.


Using Treehorn
==============

Treehorn allows you to search for information in a dictionary- or list-like object by specifying conditions. Structures that match those conditions can be returned, or they can be labeled. If they are labeled, you can use those labels to build more complex searches later, or retrieve the data. The style of Treehorn is somewhat like JQuery and similar languages that are good for manipulating tree-like data structures such as web pages.

We'll explain Treehorn by stepping through an example of how we would extract data from the following JSON blob:


::


        {
            "source": "users",
            "hash": "Ch8KFgjQj67igOnVto4BELHgwMD7iNfjkQEYlrfjtZAt",
            "events": [
                {
                    "appName": "mobileapp",
                    "browser": {
                        "name": "Google Chrome",
                        "version": []
                    },
                    "duration": 0,
                    "created": 1550596005797,
                    "location": {
                        "country": "United States",
                        "state": "Massachusetts",
                        "city": "Boston"
                    },
                    "id": "af6de71b",
                    "smtpId": null,
                    "portalId": 537105,
                    "email": "alice@gmail.com",
                    "sentBy": {
                        "id": "befa29c9",
                        "created": 1550518557458
                    },
                    "type": "OPEN",
                    "filteredEvent": false,
                    "deviceType": "COMPUTER"
                },
                {
                    "appName": "desktopapp",
                    "browser": {
                        "name": "Firefox",
                        "version": []
                    },
                    "duration": 0,
                    "created": 1550596005389,
                    "location": {
                        "country": "United States",
                        "state": "New York",
                        "city": "New York"
                    },
                    "id": "12aadd80",
                    "smtpId": null,
                    "portalId": 537105,
                    "email": "bob@gmail.com",
                    "sentBy": {
                        "id": "2cd1e257",
                        "created": 1550581974777
                    },
                    "type": "OPEN",
                    "filteredEvent": false,
                    "deviceType": "COMPUTER"
                }
            ]
        }


As you can see, this JSON blob is similar to a typical response from a REST API (in fact, this is actually an example from a real REST API, with all personal information deleted).

Let's suppose you need to extract the email address and corresponding city name for each entry in ``events``. This example is simple enough that you might not see the usefulness of Treehorn, but it's complex enough to get a sense of how Treehorn works. Later, we'll look at circumstances where Treehorn's declarative style is especially useful.

There are three kinds of classes that are important for Treehorn:

1. ``Conditions`` -- These are classes that test a particular location in a tree (e.g. a dictionary) for some condition. Examples of useful conditions are being a dictionary with a certain key, being a non-empty list, having an integer value, and so on.
#. ``Traversal`` -- These classes move throughout a tree, recursively applying tests to each node that they visit. Traversals can be upward (toward the root) or downward (toward the leaves).
#. ``Label`` -- These are nothing more than strings that are attached to particular locations in the tree. Typically, we apply a label to locations in the tree that match particular conditions.
#. ``Relations`` -- Finally, this class represents n-tuples of locations in the tree. For example, if an email address is present in the tree, and the user's city is present, a ``Relation`` can be used to denote that the person with that email address lives in that city.

The workflow for a typical Treehorn query is that we (1) define some conditions (such as being an email address field); (2) traverse the tree, searching for locations that match those conditions; (3) label those locations; and (4) define a relationship from those labels, which we can use to extract the right information. We'll gradually build up a query by adding each of these steps one at a time.


Condition objects
-----------------


For this example, let's suppose you've loaded the JSON into a dictionary, like so:

::

    import json

    with open('./sample_api_response.json', 'r') as infile:
        api_response = json.load(infile)


Let's extract the email addresses and corresonding cities for each user in the API response. First, we create a couple of ``Condition`` objects using the built-in class ``HasKey``:

::

    has_email_key = HasKey('email')
    has_city_key = HasKey('city')


The ``HasKey`` class is a subclass of ``MeetsCondition``, all of which are callable and return ``True`` or ``False``. For example, you could do the following:

::

    d = {'email': 'myemail.com', 'name': 'carol'}
    has_email_key(d)  # Returns True
    has_city_key(d)   # Returns False


What if you want to test for two conditions on a single node? ``MeetsCondition`` objects can be combined into larger boolean expressions using ``&``, ``|``, and ``~`` like so:

::

    (has_email_key & has_city_key)(d)    # Returns False
    (has_email_key & ~ has_city_key)(d)  # Returns True
    (has_email_key | has_city_key)(d)    # Returns True


Traversal objects
-----------------

``MeetsCondition`` objects aren't very useful unless they're combined with traversals. There are two types of traversal classes: ``GoUp`` and ``GoDown``. Each takes a ``MeetsCondition`` object as a parameter. For example, if you want to search from the root of the tree for every location that is a dictionary with the ``email`` key, the traversal is:

::

    find_email = GoDown(condition=has_email_key)  # or GoDown(condition=HasKey('email'))


Similarly for finding places with a ``city`` key:

::

    find_city = GoDown(condition=has_city_key)  # or GoDown(condition=HasKey('city'))


If you want to retrieve all of ``find_city``'s matches, you can use its ``matches`` method, which will yield each match:

::

    for match in has_email_key.matches(api_response):
        print(match)

which will yield:

::

    {'id': 'af6de71b', 'portalId': 537105, 'location': {'state': 'Massachusetts', 'city': 'Boston', 'country': 'United States'}, 'type': 'OPEN', 'sentBy': {'id': 'befa29c9', 'created': 1550518557458}, 'appName': 'mobileapp', 'duration': 0,'smtpId': None, 'deviceType': 'COMPUTER', 'created': 1550596005797, 'email': 'alice@gmail.com', 'browser': {'version': [], 'name': 'Google Chrome'}, 'filteredEvent': False}
    {'id': '12aadd80', 'portalId': 537105, 'location': {'state': 'New York', 'city': 'New York', 'country': 'United States'}, 'type': 'OPEN', 'sentBy': {'id': '2cd1e257', 'created': 1550581974777}, 'appName': 'desktopapp', 'duration': 0, 'smtpId': None, 'deviceType': 'COMPUTER', 'created': 1550596005389, 'email': 'bob@gmail.com', 'browser': {'version': [], 'name': 'Firefox'}, 'filteredEvent': False}


Examining each of the dictionaries, we see that they do in fact contain an ``email`` key. Note that the traversal does **not** return the email string itself -- we asked only for the dictionary containing the key. This is by design, as we will see soon.

Because we want to retrieve not only the email addresses but also the cities, we need another traversal. Each of the two dictionaries containing the ``email`` key also have a subdictionary that contains a ``city`` key. So we need a second traversal to get that subdictionary. In other words, retrieving the data we need is a two-step process:

1. Starting at the root of the tree, we traverse downward until we find a dictionary with the ``email`` key.
#. From each of those dictionaries, we go down until we find a dictionary with the ``city`` key.

Any non-trivial ETL task involving nested dictionary-like objects will require multi-stage traversals like this one. So Treehorn allows you to chain traversals together using the ``>`` operator:

::

    chained_traversal = find_email > find_city


The ``chained_traversal`` says, in effect, "Go down into the tree and find every node that has an ``email`` key. Then, from each of those, continue to go down until you find a node that contains a ``city`` key. In pseudo-code:

::

    For each node_1 starting at the root:
        if node_1 has ``email`` key:
            for each node_2 starting at node_1:
                if node_2 has ``city`` key:
                    return


So far, we have set set up multi-stage searches for nodes in a tree that satisfy various conditions. Next, we have to extract the right data from those searches. This is where the ``Label`` and ``Relation`` classes come into play.

Labels
------

When nodes are identified that satisfy certain conditions, we will want to label those nodes so that we can extract data from them later. The mechanism for doing this is to use a "label".

Continuing the example, let's use the labels "email" and "city" to mark the respective nodes in the two-stage traversal. We do so by adding a label to the traversal chain. Recall that in the previous section, we wrote:

::

    chained_traversal = find_email > find_city


whereas we now have:

::

    chained_traversal = find_email + 'email' > find_city + 'city'


We use ``+`` to add a label, and the label is just a string. Under the hood, Treehorn is instantiating a ``Label`` object, but ordinarily, you shouldn't have to do that directly.

Relations
---------

Lastly, we define a ``Relation`` object to extract the data from our search. In this example, we might think of the search as returning data about people who live in a certain city. So we might name the ``Relation`` "FROM_CITY".We'll want to extract the value of the ``email`` key from the node labeled with ``email``, and similarly with the ``city`` node. This is accomplished by adding a little more syntax:

::

    Relation('FROM_CITY') == (
        (find_email + 'email')['email'] > (find_city + 'city')['city'])


After executing that statement, Treehorn will create an object named ``FROM_CITY``, which can be called on a dictionary to yield the information we want, like so:

::

    for email_city in FROM_CITY(api_response):
        print(email_city)


which will give us:

::

    {'city': 'Boston', 'email': 'alice@gmail.com'}
    {'city': 'New York', 'email': 'bob@gmail.com'}


Voila!


Summing up
==========

Normally, ETL pipelines that extract data from dictionary-like objects involve a lot of loops and hard-coded keypaths. To accomplish the simple task of extracting emails and city names from our sample JSON blob, we'd probably hard-code paths for each specific key and value, and then we'd loop over various levels in the dictionary. This has several disadvantages:

1. It leads to brittle code. If the JSON blob changes structure in even very small ways, the hard-coded paths become obsolete and have to be rewritten.
#. The code is difficult to understand and debug. Given a whole bunch of nested loops and hard-coded keypaths, it's very difficult to understand the intent of the code. Errors have to be found by painstakingly stepping through the execution.
#. It is very difficult to accommodate JSON blobs with variable structure. Some JSON blobs returned from APIs have unpredictable levels of nesting, for example. Therefore, keypaths cannot be hard-coded and recursive searches have to be written, which are inefficient and difficult to debug.

The approach taken by Treehorn alleviates some of this pain. For example, the ``GoDown`` traversal doesn't care how many levels down in the tree it must search; so it is often able to cope with inconsistent structures (within reason) without any code changes. It's also much easier to understand. You can tell from glancing at the code that the intention is to search for a dictionary with a key, and then search from there for lower-level dictionaries with another key, and return the results. Treehorn is also more efficient than writing loops and keypaths because all of its evaluations are lazy -- it doesn't hold partial results in memory any longer than necessary because everything is yielded by generators.
