from setuptools import setup, find_packages

setup(
    name='pypeliner',
    version='1.0.0',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    description='Super easy python framework/library for building stream-like pipelines efficiently and fast. Great for the readability and maintainability.',
    author='Sadig Akhund',
    author_email='sadigaxund@gmail.com',
    url='https://github.com/sadigaxund/pypeliner',
    classifiers=[
        'Programming Language :: Python :: 3.11.7',
        'License :: OSI Approved :: Unlicense',
        'Operating System :: Linux',
    ],
)
