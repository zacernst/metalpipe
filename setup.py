import setuptools

setuptools.setup(
    name="nanostream",
    version="0.1.5",
    author="Zachary Ernst",
    author_email="zac.ernst@gmail.com",
    description="Small-scale stream processing for ETL",
    url="https://github.com/zacernst/nanostream",
    packages=[
        'nanostream', 'nanostream.message', 'nanostream.node_queue',
        'nanostream.node_classes',
        'nanostream.watchdog', 'nanostream.utils', 'nanostream.exp',],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
