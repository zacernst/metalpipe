import setuptools

setuptools.setup(
    name="metalpipe",
    version="0.1.15",
    author="Zachary Ernst",
    author_email="zac.ernst@gmail.com",
    description="Modules for ETL Pipelines",
    url="https://github.com/zacernst/metalpipe",
    scripts=['metalpipe/bin/metalpipe'],
    packages=[
        'metalpipe',
        'metalpipe.message',
        'metalpipe.node_queue',
        'metalpipe.node_classes',
        'metalpipe.watchdog',
        'metalpipe.utils',
        'metalpipe.exp',],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
      'alabaster==0.7.11',
      'argh==0.26.2',
      'asn1crypto==0.24.0',
      'atomicwrites==1.2.1',
      'attrs==18.2.0',
      'Babel==2.6.0',
      'bcrypt==3.1.4',
      'behave==1.2.6',
      'boto3==1.7.62',
      'botocore==1.10.62',
      'certifi==2018.4.16',
      'cffi==1.11.5',
      'chardet==3.0.4',
      'civis==1.7.2',
      'click==6.7',
      'cloudpickle==0.5.3',
      'cryptography==2.3',
      'datadog==0.26.0',
      'decorator==4.3.0',
      'docutils==0.14',
      'graphviz==0.10.1',
      'gunicorn==19.9.0',
      'idna==2.7',
      'imagesize==1.1.0',
      'Jinja2==2.10.1',
      'jmespath==0.9.3',
      'joblib==0.11',
      'jsonref==0.1',
      'jsonschema==2.6.0',
      'kafka==1.3.1',
      'MarkupSafe==1.0',
      'prometheus_client',
      'more-itertools==4.3.0',
      'networkx==2.1',
      'packaging==18.0',
      'paramiko==2.4.2',
      'parse==1.8.4',
      'parse-type==0.4.2',
      'pathlib2==2.3.2',
      'pathtools==0.1.2',
      'pkginfo==1.4.2',
      'pluggy==0.7.1',
      'pockets==0.6.2',
      'prettytable==0.7.2',
      'pubnub==4.1.0',
      'py==1.6.0',
      'pyasn1==0.4.3',
      'pycparser==2.18',
      'pycryptodomex==3.6.4',
      'Pygments==2.7.4',
      'PyNaCl==1.2.1',
      'pyparsing==2.2.1',
      'pytest==3.8.1',
      'python-dateutil==2.7.3',
      'python-dotenv==0.6.0',
      'pytz==2018.5',
      'pyyaml>=3.0',
      'requests==2.20',
      'requests-toolbelt==0.8.0',
      's3fs==0.1.5',
      's3transfer==0.1.13',
      'schedule==0.5.0',
      'six==1.11.0',
      'snowballstemmer==1.2.1',
      'Sphinx==1.8.1',
      'sphinx-rtd-theme==0.4.2',
      'sphinxcontrib-napoleon==0.6.1',
      'sphinxcontrib-websupport==1.1.0',
      'sqlparse==0.2.4',
      'TimedDict==0.2.4',
      'timeddictionary==0.1.1',
      'tqdm==4.23.4',
      'twine==1.11.0',
      'urllib3==1.23',
      'watchdog==0.8.3',
      'yapf==0.24.0']

)
