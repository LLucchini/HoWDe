from setuptools import setup, find_packages

setup(
    name='HoWDe',
    version='0.1.1',
    author='Silvia De Sojo Caso, Lorenzo Lucchini, Laura Alessandretti',
    author_email='sdesojoc@gmail.com, lorenzo.f.lucchini.work@gmail.com',
    description='A package for detecting home and work locations from individual timestamped sequences of stop locations.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/LLucchini/HoWDe',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
	install_requires=[
	    'numpy>=1.26.0', 			# 1.26.4
	    'pandas>=2.2.0', 			# 2.2.3
	    'python-dateutil>=2.9.0', 	# 2.9.0
	    'tqdm>=4.67.0', 			# 4.67.1
	    'pyspark>=3.5.0', 			# 3.5.4
	],
)