==============
kvk_url_finder
==============

Een tool om urls bij een kvk entries met postcodes te vinden


Omschrijving
============

Deze tool is nog in ontwikkeling


Installatie
===========

Om te installeren doe::

    python setup.py sdist bdist_wheel

Hiermee wordt wheel file onder de dist directory gezet. Als je de wheel wilt installeren, doe::

    pip install dist/kvk_url_finder-versienummer-none-any.whl

Met dit command wordt kvk_url_finder utility onder the python installatie folder gezet. Mocht je
een ander folder willen gebruiken om te installeren, doe dan::

    pip install dist/kvk_url_finder-versienummer-none-any.whl --prefix prefix-dir

Om zo'n installatie in een alternatieve locatie weer ongedaan te maken moet je de prefix via een
environment variable meegeven om pip op de hoogte te stellen waar hij de package kan vinden::

    PYTHONUSERBASE prefix-dir pip uninstall kvk_url_finder

Note
====

This project has been set up using PyScaffold 3.0.3. For details and usage
information on PyScaffold see http://pyscaffold.org/.
