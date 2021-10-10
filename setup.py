from setuptools import setup, find_packages

VERSION = '1.0.1' 
DESCRIPTION = 'Python package for creating and visualizing IBKR trading logs'
LONG_DESCRIPTION = 'This package is built to help you import and parse different trading files from IBKR to track and visualize trading performance'
# Setting up
setup(
       # the name must match the folder name 'verysimplemodule'
        name="TradingLog", 
        version=VERSION,
        author="Jason Dsouza",
        author_email="<youremail@email.com>",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=[], # add any additional packages that 
        # needs to be installed along with your package. Eg: 'caer'
        
        keywords=['python', 'first package'],
        classifiers= [
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Education",
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 3",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: Microsoft :: Windows",
        ]
)