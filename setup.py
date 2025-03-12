from setuptools import setup, find_packages

__version__ = '1.0.0'

setup(
    name='fhir_etl',
    version=__version__,
    description="FHIR (Fast Healthcare Interoperability Resources) ETL.",
    long_description=open('README.md').read(),
    url='https://github.com/FHIR-Aggregator/FHIR-ETL',
    author='https://fhir-aggregator.org',
    packages=find_packages(),
    entry_points={
        'console_scripts': ['fhir_etl = fhir_etl.cli:cli']
    },
    install_requires=[
        'charset_normalizer',
        'idna',
        'certifi',
        'requests',
        'pydantic',
        'pytest',
        'click',
        'pathlib',
        'orjson',
        'tqdm',
        'uuid',
        'openpyxl',
        'pandas',
        'inflection',
        'iteration_utilities',
        'gen3-tracker>=0.0.7rc2',
        'fhir.resources==8.0.0b4'  # FHIR® (Release R5, version 5.0.0)
    ],
    tests_require=['pytest'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.13',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics'
    ],
    platforms=['any'],
    python_requires='>=3.13, <4.0',
)

