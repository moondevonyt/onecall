#!/bin/sh
echo ------generating dist file----
python setup.py sdist
echo ------uploading dist file to pip----
twine upload dist/*