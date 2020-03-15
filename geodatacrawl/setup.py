import sys
from setuptools import setup

setup(
    name = "Adaptive Geolocated-data crawler",        
    version = "1.0",
    packages=["geodatacrawl"],    
                                
    dependency_links = [],      
    install_requires=[],
    extras_require={},      
                            
    package_data = {},
    author="Domenico Monaco",
    author_email = "monaco.d@gmail.com",
    description = "Adaptive Geolocated-Data crawler agent for Google Place",
    license = "None",
    keywords= "",
    url = "https://github.com/domenicomonaco/adaptive_geo_data_crawl",
    entry_points = {
        "console_scripts": [        
            "adaptive_geo_data_crawl = adaptive_geo_data_crawl.main:main",
        ],
        "gui_scripts": []
    }
)
