from setuptools import setup, find_packages

VERSION = '0.0.1'
DESCRIPTION = 'For crypto algorithm trading'
LONG_DESCRIPTION = 'hmv-onecall library is used to connect and trade with cryptocurrency exchanges'

# Setting up
setup(
    # the name must match the folder name 'verysimplemodule'
    name="hmv-onecall",
    version=VERSION,
    author="Joshy Joy",
    author_email="joshyjoy999@gmail.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=[
        'setuptools>=60.9.0',
        'requests>=2.18.4',
    ],

    keywords=['python', 'onecall'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: Information Technology",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)
