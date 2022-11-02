from setuptools import setup, find_packages

VERSION = '1.0.10'
DESCRIPTION = "An algorithmic trading python package to save thousands of hours with the quickest crypto trading library so you can easily algo trade on any exchange & execute with unified function calls"
LONG_DESCRIPTION = "the onecall trading bot python library saves you thousands of hours to connect and trade with cryptocurrency exchanges. Taking out all of the hard work of connected to many exchanges and figuring out their documentation. with onecall you simply connect to one call and all of the functions for each exchange are the same so you save time and don't have to read new docs. onecall has a ton of custom algo trading functions built in saving you thousands of hours of time"

# Setting up
setup(
    # the name must match the folder name 'verysimplemodule'
    name="onecall",
    version=VERSION,
    author="Moon Dev",
    author_email="moondevonyt@gmail.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=[
        'setuptools>=60.9.0',
        'requests>=2.18.4',
    ],

    keywords=['python', 'onecall', 'trading bot','quant', 'trading', 'trading algo', 'algorithmic trading', 'algo trading', 'crypto', 'exchange', 'automate trading', 'pinescript', 'mev', 'crypto exchange', 'binance', 'ftx', 'ethereum', 'solana', 'bitcoin', 'coinbase', 'phemex', 'bybit'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: Information Technology",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ],
    include_package_data=True
)
